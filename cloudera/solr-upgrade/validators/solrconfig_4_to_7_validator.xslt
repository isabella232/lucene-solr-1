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

<xsl:template match="indexConfig">
  <xsl:if test="./termIndexInterval">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-6560</jira_number>
      <description>The "termIndexInterval" option is a no-op and should be removed.</description>
      <recommendation>Remove this no longer used configuration.</recommendation>
      <reindexing>no</reindexing>
      <transform>yes</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="./checkIntegrityAtMerge">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-6834</jira_number>
      <description>The "checkIntegrityAtMerge" option is a no-op and should be removed.</description>
      <recommendation>Remove this option, it is now done automatically, internally.</recommendation>
      <reindexing>no</reindexing>
      <transform>yes</transform>
    </incompatibility>
  </xsl:if>

  <xsl:if test="./nrtMode">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-6897</jira_number>
      <description>The &lt;nrtMode&gt; configuration has been discontinued and should be removed.</description>
      <recommendation>Solr defaults to using NRT searchers and this configuration is not required.</recommendation>
      <reindexing>No</reindexing>
      <transform>yes</transform>
    </incompatibility>
  </xsl:if>
    
  <xsl:if test="./mergePolicy or ./mergeFactor or ./maxMergeDocs">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-8621</jira_number>
      <description>The &lt;mergePolicy&gt; and &lt;mergeFactor&gt; and &lt;maxMergeDocs&gt; elements have been removed in favor of the &lt;mergePolicyFactory&gt;</description>
      <recommendation>Configure mergePolicyFactory instead.</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="jmx">
  <incompatibility>
    <level>warning</level>
    <jira_number>SOLR-9959</jira_number>
    <description>&lt;jmx&gt; element in solrconfig.xml is no longer supported.</description>
    <recommendation>Equivalent functionality can be configured in solr.xml using &lt;metrics&gt;&lt;reporter ...&gt; element and SolrJmxReporter implementation.</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="infoStream">
  <xsl:if test="@file">
    <incompatibility>
      <level>error</level>
      <jira_number>TBD</jira_number>
      <description>The "file" attribute of infoStream element is removed</description>
      <recommendation>Control this via your logging configuration (org.apache.solr.update.LoggingInfoStream) instead.</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="updateRequestProcessorChain">
  <xsl:if test="./processor[@class='org.apache.solr.update.processor.UniqFieldsUpdateProcessorFactory']/lst[@name='fields']">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-4249</jira_number>
      <description>UniqFieldsUpdateProcessorFactory no longer supports the &lt;lst named=&quot;fields&quot;&gt; init param style.</description>
      <recommendation>Update your solrconfig.xml to use &lt;arr name=&quot;fieldName&quot;&gt; instead.</recommendation>
      <reindexing>no</reindexing>
      <transform>yes</transform>
    </incompatibility>
  </xsl:if>
  <xsl:if test="./processor[@class='solr.UpdateIndexAuthorizationProcessorFactory']">
    <incompatibility>
      <level>error</level>
      <jira_number>SENTRY-1475</jira_number>
      <description>UpdateRequestProcessorFactory of type solr.UpdateIndexAuthorizationProcessorFactory is removed</description>
      <recommendation>Remove the reference of this factory from the updateRequestProcessorChain</recommendation>
      <reindexing>No</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>

  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="config">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-10494</jira_number>
      <description>The default response type is now JSON ("wt=json") instead of XML, and line indentation is now on by default ("indent=on").</description>
      <recommendation> If you expect the responses to your queries to be returned in the previous format (XML format, no indentation), you must now you must now explicitly pass in "wt=xml" and "indent=off" as query parameters, or configure them as defaults on your request handlers.</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
    <xsl:if test="not(./schemaFactory)">
      <incompatibility>
        <level>info</level>
        <jira_number>SOLR-8131</jira_number>
        <description>The implicit default schema factory is changed from ClassicIndexSchemaFactory to ManagedIndexSchemaFactory. This means that the Schema APIs ( /&lt;collection&gt;/schema ) are enabled and the schema is mutable.</description>
        <recommendation>Users who wish to preserve back-compatible behavior should either explicitly configure schemaFactory to use ClassicIndexSchemaFactory, or ensure that the luceneMatchVersion for the collection is less then 6.0</recommendation>
        <reindexing>no</reindexing>
        <transform>no</transform>
      </incompatibility>
    </xsl:if>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="searchComponent">
    <xsl:if test="./highlighting[@class='org.apache.solr.highlight.PostingsSolrHighlighter']">
    <incompatibility>
      <level>warning</level>
      <jira_number>SOLR-10700</jira_number>
      <description>The PostingsSolrHighlighter is deprecated and is now part of the UnifiedSolrHighlighter.</description>
      <recommendation>Change configuration to use the UnifiedSolrHighlighter instead.</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

<xsl:template match="searchComponent[@class='org.apache.solr.handler.component.QueryIndexAuthorizationComponent'
                                     or @class='org.apache.solr.handler.component.SecureRealTimeGetComponent']">
  <incompatibility>
    <level>error</level>
    <jira_number>SENTRY-1475</jira_number>
    <description>Search component (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed.</description>
    <recommendation>Remove the configuration of this search component and update configuration of request handlers referring to it</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
</xsl:template>

<xsl:template match="requestHandler">
   <xsl:if test="@class='solr.admin.AdminHandlers'">
    <incompatibility>
      <level>info</level>
      <jira_number>TBD</jira_number>
      <description>The AdminHandlers class has been deprecated and removed.</description>
      <recommendation>Remove this handler configuration.</recommendation>
      <reindexing>no</reindexing>
      <transform>yes</transform>
    </incompatibility>
  </xsl:if>
  <!-- TODO - avoid duplication of logic -->
  <xsl:if test="@class='solr.SecureRealTimeGetHandler'">
    <incompatibility>
      <level>error</level>
      <jira_number>SENTRY-1475</jira_number>
      <description>Request handler (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed.</description>
      <recommendation>Update request handler class name to solr.RealTimeGetHandler</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:if test="@class='solr.SecureFieldAnalysisRequestHandler'">
    <incompatibility>
      <level>error</level>
      <jira_number>SENTRY-1475</jira_number>
      <description>Request handler (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed.</description>
      <recommendation>Update request handler class name to solr.FieldAnalysisRequestHandler</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:if test="@class='solr.SecureDocumentAnalysisRequestHandler'">
    <incompatibility>
      <level>error</level>
      <jira_number>SENTRY-1475</jira_number>
      <description>Request handler (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed.</description>
      <recommendation>Update request handler class name to solr.DocumentAnalysisRequestHandler</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:if test="@class='solr.SecureReplicationHandler'">
    <incompatibility>
      <level>error</level>
      <jira_number>SENTRY-1475</jira_number>
      <description>Request handler (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed.</description>
      <recommendation>Update request handler class name to solr.ReplicationHandler</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
  </xsl:if>
  <xsl:if test="@class='org.apache.solr.handler.dataimport.DataImportHandler'">
    <incompatibility>
      <level>info</level>
      <jira_number>CDH-26966</jira_number>
      <description>Request handler (name = <xsl:value-of select="attribute::name"/> and class = <xsl:value-of select="attribute::class"/>) is removed as it is not supported in Cloudera Search</description>
      <recommendation>Remove this request handler configuration. Refer to Cloudera Search docs for the alternatives</recommendation>
      <reindexing>no</reindexing>
      <transform>yes</transform>
    </incompatibility>
  </xsl:if>
</xsl:template>

<xsl:template match="requestDispatcher">
    <xsl:if test="not(@handleSelect)">
    <incompatibility>
      <level>info</level>
      <jira_number>SOLR-3161</jira_number>
      <description>&lt;requestDispatcher handleSelect="..."&gt; now defaults to false when luceneMatchVersion >= 7.0, thus ignoring "qt".</description>
      <recommendation>TBD</recommendation>
      <reindexing>no</reindexing>
      <transform>no</transform>
    </incompatibility>
      </xsl:if>
  <xsl:apply-templates select="child::node()"/>
</xsl:template>

</xsl:stylesheet>
