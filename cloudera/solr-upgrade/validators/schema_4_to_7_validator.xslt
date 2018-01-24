<xsl:stylesheet version="2.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="true" standalone="yes" />
<xsl:strip-space elements="*"/>

<xsl:template match="/">
  <xsl:element name="result">
  <xsl:apply-templates/>
  </xsl:element>
</xsl:template>

<!-- identity transform -->
<xsl:template match="@* | node()">
  <xsl:apply-templates select="node() | @*"/>
</xsl:template>

<xsl:template match="fieldType">
  <xsl:if test="@class='solr.SortableIntField'
                or @class='solr.SortableLongField'
                or @class='solr.SortableFloatField'
                or @class='solr.SortableDoubleField'
                or @class='solr.DateField'
                or @class='solr.IntField'
                or @class='solr.LongField'
                or @class='solr.FloatField'
                or @class='solr.DoubleField'
                or @class='solr.BCDIntField'
                or @class='solr.BCDLongField'
                or @class='solr.BCDStrField'">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-5936</jira_number>
      <description>Legacy field type (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed.</description>
      <recommendation>TBD</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="(@class='solr.SpatialRecursivePrefixTreeFieldType'
                 or @class='solr.SpatialPointVectorFieldType'
                 or @class='solr.BBoxField') and @units">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-6797</jira_number>
      <description>'units' attribute for spatial field type (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select
="attribute::class"/>) is deprecated</description>
      <recommendation>
        Spatial fields originating from Solr 4 (e.g. SpatialRecursivePrefixTreeFieldType, BBoxField)
        have the 'units' attribute deprecated, now replaced with 'distanceUnits'.  If you change it to
        a unit other than 'degrees' (or if you don't specify it, which will default to kilometers if
        geo=true), then be sure to update maxDistErr as it's in those units.  If you keep units=degrees
        then it should be backwards compatible but you'll get a deprecation warning on startup.
      </recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>


<xsl:template match="field">

<!-- BAD EXAMPLE:
  <field name="id" type="string" indexed="true" stored="true"/>
  <field name="_root_" type="int" indexed="true" stored="false"/>
-->
  <xsl:if test="@name='_root_' and //field[@name='id']/@type != @type">
      <incompatibility>
      <level>error</level>
      <jira_number></jira_number>
      <description>_root_ field must be defined using the exact same type as the uniqueKey field</description>
      <recommendation>
        Change the schema so that the "type" attribute of the _root_ field matches the "type" attribute of the "id" field.
      </recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="charFilter">
  <xsl:if test="@class='solr.LegacyHTMLStripCharFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>TODO</jira_number>
      <description>LegacyHTMLStripCharFilterFactory has been removed</description>
      <recommendation>TBD</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="filter">
  <xsl:if test="@class='solr.BeiderMorseFilterFactory' and //field/@type=../../@name">
    <incompatibility>
      <level>info</level>
      <jira_number>LUCENE-6058</jira_number>
      <description>Users of the BeiderMorseFilterFactory will need to rebuild their indexes after upgrading</description>
      <recommendation>
        Due to changes in the underlying commons-codec package, users of the BeiderMorseFilterFactory will need to rebuild their indexes after upgrading.
      </recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.ThaiWordFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-4984</jira_number>
      <description>ThaiWordFilterFactory has been removed</description>
      <recommendation>Replace with ThaiTokenizerFactory</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.SmartChineseWordTokenFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-4984</jira_number>
      <description>SmartChineseWordTokenFilterFactory has been removed</description>
      <recommendation>Replace with HMMChineseTokenizerFactory</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.ICUCollationKeyFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>ICUCollationKeyFilterFactory has been removed</description>
      <recommendation>Use ICUCollationKeyAnalyzer instead</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.CollationKeyFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>CollationKeyFilterFactory has been removed</description>
      <recommendation>Use CollationKeyAnalyzer instead</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.PositionFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>PositionFilterFactory has been removed</description>
      <recommendation>There is no replacement for this filter.</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.SlowSynonymFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>SlowSynonymFilterFactory has been removed</description>
      <recommendation>Use SynonymFilterFactory instead</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.FSTSynonymFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>FSTSynonymFilterFactory has been removed</description>
      <recommendation>Use SynonymFilterFactory instead</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.ChineseFilterFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>ChineseFilterFactory has been removed</description>
      <recommendation>Use StopFilterFactory instead, which has the same functionality.</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>





<xsl:template match="tokenizer">

  <xsl:if test="@class='solr.SmartChineseSentenceTokenizerFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-4984</jira_number>
      <description>SmartChineseSentenceTokenizerFactory has been removed</description>
      <recommendation>Replace with HMMChineseTokenizer</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

   <xsl:if test="@class='solr.ArabicLetterTokenizerFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>ArabicLetterTokenizerFactory has been removed</description>
      <recommendation>Use StandardTokenizerFactory instead</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.CJKTokenizerFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>LUCENE-5944</jira_number>
      <description>CJKTokenizerFactory has been removed</description>
      <recommendation>Use StandardTokenizer, CJKWidthFilter, CJKBigramFilter, and LowerCaseFilter instead</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='solr.RussianLetterTokenizerFactory'">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-1410</jira_number>
      <description>RussianLetterTokenizerFactory has been removed</description>
      <recommendation>Replace with StandardTokenizerFactory</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>




<xsl:template match="analyzer">

  <xsl:if test="@class='org.apache.lucene.analysis.cn.ChineseAnalyzer'">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-1410</jira_number>
      <description>ChineseAnalyzer has been removed</description>
      <recommendation>Use StandardAnalyzer instead, which has the same functionality.</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='org.apache.lucene.analysis.snowball.SnowballAnalyzer'">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-1410</jira_number>
      <description>SnowballAnalyzer has been removed</description>
      <recommendation>Use the appropriate language-specific analyzer instead.</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="@class='org.apache.lucene.analysis.miscellaneous.PatternAnalyzer'">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-1410</jira_number>
      <description>PatternAnalyzer has been removed</description>
      <recommendation>use the pattern-based analysis in the analysis/pattern package instead.</recommendation>
      <reindexing>Required</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>



<xsl:template match="schema">
  <xsl:if test="not(./similarity)">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-8270, SOLR-8271</jira_number>
      <description>The implicit default Similarity is changed to SchemaSimilarityFactory</description>
      <recommendation>Users who wish to preserve back-compatible behavior should either explicitly configure ClassicSimilarityFactory, or ensure that the luceneMatchVersion for the collection is less then 6.0</recommendation>
      <reindexing>TBD</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="similarity[@class='solr.DefaultSimilarityFactory']">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-8239</jira_number>
      <description>DefaultSimilarityFactory has been removed</description>
      <recommendation>If you currently have DefaultSimilarityFactory explicitly referenced in your schema.xml, edit your config to use the functionally identical ClassicSimilarityFactory</recommendation>
      <reindexing>TBD</reindexing>
      <transform>yes</transform>
    </incompatibility>
</xsl:template>

<xsl:template match="similarity[@class='solr.SchemaSimilarityFactory']">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-8261, SOLR-8329</jira_number>
      <description>SchemaSimilarityFactory has been modified to use BM25SimilarityFactory as the default for fieldTypes that do not explicitly declare a Similarity</description>
      <recommendation>The legacy behavior of using ClassicSimilarity as the default will occur if the luceneMatchVersion for the collection is less then 6.0, or the 'defaultSimFromFieldType' configuration option may be used to specify any default of your choosing</recommendation>
      <reindexing>TBD</reindexing>
      <transform>no</transform>
    </incompatibility>
</xsl:template>


<!-- BAD EXAMPLE:
  <solrQueryParser defaultOperator="OR"/>
-->
<xsl:template match="solrQueryParser">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-10584</jira_number>
      <description>Setting solrQueryParser defaultOperator is no longer supported.</description>
      <recommendation>Please use "q.op" parameter on the request instead.</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
</xsl:template>

<!-- BAD EXAMPLE:
  <defaultSearchField>id</defaultSearchField>
-->
<xsl:template match="defaultSearchField">
    <incompatibility>
      <level>error</level>
      <jira_number>SOLR-10585</jira_number>
      <description>Setting defaultSearchField in the schema is no longer supported.</description>
      <recommendation>Please use "df" parameter on the request instead.</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
</xsl:template>



</xsl:stylesheet>
