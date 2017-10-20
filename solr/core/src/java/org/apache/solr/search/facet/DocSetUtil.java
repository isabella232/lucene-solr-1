package org.apache.solr.search.facet;

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


import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

class DocSetUtil {

    public static void collectSortedDocSet(DocSet docs, IndexReader reader, Collector collector) throws IOException {
        // TODO add SortedDocSet sub-interface and take that.
        // TODO collectUnsortedDocSet: iterate segment, then all docSet per segment.

        final List<AtomicReaderContext> leaves = reader.leaves();
        final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
        int segBase = 0;
        int segMax;
        int adjustedMax = 0;
        AtomicReaderContext ctx = null;
        for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
            final int doc = docsIt.nextDoc();
            if (doc >= adjustedMax) {
                do {
                    ctx = ctxIt.next();
                    segBase = ctx.docBase;
                    segMax = ctx.reader().maxDoc();
                    adjustedMax = segBase + segMax;
                } while (doc >= adjustedMax);
                collector.setNextReader(ctx);
            }
            if (doc < segBase) {
                throw new IllegalStateException("algorithm expects sorted DocSet but wasn't: " + docs.getClass());
            }
            collector.collect(doc - segBase);  // per-seg collectors
        }
    }
}
