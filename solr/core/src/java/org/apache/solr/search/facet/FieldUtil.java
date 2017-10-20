/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search.facet;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;

/** @lucene.internal
 * Porting helper... may be removed if it offers no value in the future.
 */
public class FieldUtil {

  public static int FIXED_BIT_SET_NO_MORE_DOCS = -1;  // BP - Later versions of FixedBitSet DocIdSetIterator.NO_MORE_DOCS is MAX_INT in later versions of

  /** Simpler method that creates a request context and looks up the field for you */
  public static SortedDocValues getSortedDocValues(SolrIndexSearcher searcher, String field) throws IOException {
    SchemaField sf = searcher.getSchema().getField(field);
    QueryContext qContext = QueryContext.newContext(searcher);
    return getSortedDocValues( qContext, sf, null );
  }


  public static SortedDocValues getSortedDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    SortedDocValues si = null;
    if (!field.hasDocValues() && (field.getType() instanceof StrField || field.getType() instanceof TextField)) {
      si = FieldCache.DEFAULT.getTermsIndex(context.searcher().getAtomicReader(), field.getName());
    } else if (field.hasDocValues()) {
      si = context.searcher().getAtomicReader().getSortedDocValues( field.getName() );
    }

    return si == null ? DocValues.emptySorted() : si;
  }

  public static SortedSetDocValues getSortedSetDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    // Following the same usage pattern from SimpleFacets, multi-valued non-docValue fields always use UIF
    // Passing the FieldCache top-level atomicReader will create top-level view only (as opposed to per-segment and then creating a wrapper)
    // and we already have UIF for a top-level view used in other places.
    // Wrapping UIF is also a possibility, but would be a fair amount slower.  Better to just use it directly and avoid SortedSetDocValues for non-docValue fields.
    // If we *did* want to implement this for FieldCache, we would want to do something like SlowCompositeReaderWrapper does to cache the globalOrds.
    // Later versions of Solr do this by inserting UnInvertingReader below SlowCompositeReaderWrapper to emulate docValues (and hence SlowCompositeReaderWrapper
    // caches global ord map for both FC and docValues).  We don't want to do this for this backport due to risk if introducing a bug.

    if (!field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Json Facet API can't use multi-valued docvalues for non docValue field, use uif method for " + field);

      // This is no good... doesn't go to segment level.
      // SortedSetDocValues si = FieldCache.DEFAULT.getDocTermOrds(context.searcher().getAtomicReader(), field.getName());
    }

    // The SlowCompositeReaderWrapper will fetch segment level docValues and construct a top-level view, which we need for global ords.
    SortedSetDocValues si = context.searcher().getAtomicReader().getSortedSetDocValues(field.getName());
    return si == null ? DocValues.emptySortedSet() : si;
  }


  public static SortedDocValues getSortedDocValuesSegment(QueryContext context, SchemaField field, AtomicReaderContext segmentContext) throws IOException {
    SortedDocValues si = null;
    if (!field.hasDocValues() && (field.getType() instanceof StrField || field.getType() instanceof TextField)) {
      si = FieldCache.DEFAULT.getTermsIndex(context.searcher().getAtomicReader(), field.getName());
      // if (!segmentContext.isTopLevel) {
      // provide a segment view into the top level fieldCache.  Always do it (even for a single segment index) for better inlining
      si = si == null ? null : new SortedDocValuesSeg(si, segmentContext.docBase, segmentContext.reader().maxDoc());

    } else if (field.hasDocValues()) {
      // for docValues, go directly to the segment level
      si = segmentContext.reader().getSortedDocValues( field.getName() );
    }

    return si == null ? DocValues.emptySorted() : si;
  }

  public static SortedDocValues getSortedDocValuesSegment(SchemaField sf, SortedDocValues top, AtomicReaderContext segmentContext) throws IOException {
    if (segmentContext.ord == 0 || top.getValueCount() == 0) return top;
    assert sf.hasDocValues() == false;
    return new SortedDocValuesSeg(top, segmentContext.docBase, segmentContext.reader().maxDoc());
  }

  static class SortedDocValuesSeg extends SortedDocValues {
    final SortedDocValues values;
    final int base;
    final int maxDoc;

    public SortedDocValuesSeg(SortedDocValues values, int base, int maxDoc) {
      this.values = values;
      this.base = base;
      this.maxDoc = maxDoc;
    }

    @Override
    public int getOrd(int docID) {
      assert docID < maxDoc;
      return values.getOrd(docID + base);
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return values.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return values.getValueCount();
    }
  }

  // Get docValues from FC or real docValues
  public static NumericDocValues getNumericDocValues(final SchemaField sf, final AtomicReaderContext segmentContext) throws IOException {
    // In 4.10, NumericFacets went through the FieldCache for both FC and DocValues
    // The FC wrapped DocValues (if they existed) to look like FC interface
    // Solr 5?,6,7 does the reverse, wrapping FC to make it look like DocValues
    //
    // TrieField.createFields creates the long bits for the numeric field:
    /****** for reference
    final long bits;
    if (field.numericValue() instanceof Integer || field.numericValue() instanceof Long) {
      bits = field.numericValue().longValue();
    } else if (field.numericValue() instanceof Float) {
      bits = Float.floatToIntBits(field.numericValue().floatValue());
    } else {
      assert field.numericValue() instanceof Double;
      bits = Double.doubleToLongBits(field.numericValue().doubleValue());
    }
    fields.add(new NumericDocValuesField(sf.getName(), bits));
    ******/

    if (sf.hasDocValues()) {
      return DocValues.getNumeric(segmentContext.reader(), sf.getName());
    }

    // Wrap FieldCache to make it look like docValues

    final FieldType ft = sf.getType();
    final org.apache.lucene.document.FieldType.NumericType numericType = ft.getNumericType();
    if (numericType == null) {
      throw new IllegalStateException();
    }

    switch (numericType) {
      case LONG:
        return new NumericDocValues() {
          FieldCache.Longs vals = FieldCache.DEFAULT.getLongs(segmentContext.reader(), sf.getName(), true);
          @Override
          public long get(int docID) {
            return vals.get(docID);
          }
        };
      case INT:
        return new NumericDocValues() {
          FieldCache.Ints vals = FieldCache.DEFAULT.getInts(segmentContext.reader(), sf.getName(), true);
          @Override
          public long get(int docID) {
            return vals.get(docID); // sign extension intended
          }
        };
      case FLOAT:
        return new NumericDocValues() {
          FieldCache.Floats vals = FieldCache.DEFAULT.getFloats(segmentContext.reader(), sf.getName(), true);
          @Override
          public long get(int docID) {
            return Float.floatToRawIntBits(vals.get(docID)); // sign extension intended
          }
        };
      case DOUBLE:
        return new NumericDocValues() {
          FieldCache.Doubles vals = FieldCache.DEFAULT.getDoubles(segmentContext.reader(), sf.getName(), true);
          @Override
          public long get(int docID) {
            return Double.doubleToRawLongBits(vals.get(docID));
          }
        };
      default:
        throw new AssertionError();
    }
  }

  // get docs with field from real docValues or from FC
  public static Bits getDocsWithField(final SchemaField sf, final AtomicReaderContext segmentContext) throws IOException {
    // The FieldCache version handles both docValues and FieldCache for us
    return FieldCache.DEFAULT.getDocsWithField(segmentContext.reader(), sf.getName());
  }


}
