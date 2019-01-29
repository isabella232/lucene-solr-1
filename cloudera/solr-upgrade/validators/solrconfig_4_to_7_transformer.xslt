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

<!-- identity transform -->
<xsl:template match="@* | node()">
  <xsl:copy>
    <xsl:apply-templates select="node() | @*"/>
  </xsl:copy>
</xsl:template>

<!-- Required to ensure proper indentation for the comments in the config file -->
<xsl:template match="/comment()">
  <xsl:text>&#10;</xsl:text>
  <xsl:copy/>
</xsl:template>

<xsl:template match="config">
  <xsl:text>&#10;</xsl:text>
  <xsl:copy>
    <xsl:apply-templates select="child::node()"/>
  </xsl:copy>
</xsl:template>

<!-- Remove the following configuration sections from the result -->
<xsl:template match="indexConfig/termIndexInterval">
  <xsl:message>* Removed "termIndexInterval" configuration from "indexConfig" section</xsl:message>
</xsl:template>
<xsl:template match="indexConfig/checkIntegrityAtMerge">
  <xsl:message>* Removed "checkIntegrityAtMerge" configuration from "indexConfig" section</xsl:message>
</xsl:template>
<xsl:template match="indexConfig/nrtMode">
  <xsl:message>* Removed "nrtMode" configuration from "indexConfig" section</xsl:message>
</xsl:template>
<xsl:template match="indexConfig/mergeFactor">
  <xsl:message>* Removed "mergeFactor" configuration from "indexConfig" section</xsl:message>
</xsl:template>
<xsl:template match="indexConfig/mergePolicy">
  <xsl:message>* Removed "mergeFactor" configuration from "indexConfig" section</xsl:message>
</xsl:template>
<xsl:template match="indexConfig/maxMergeDocs">
  <xsl:message>* Removed "mergeFactor" configuration from "indexConfig" section</xsl:message>
</xsl:template>

<!-- Update UniqFieldsUpdateProcessorFactory config section to use correct param name -->
<xsl:template match="updateRequestProcessorChain/processor[@class='org.apache.solr.update.processor.UniqFieldsUpdateProcessorFactory']/lst[@name='fields']">
  <xsl:message>* Using &lt;arr name="fieldName"&gt; init param style for UniqFieldsUpdateProcessorFactory</xsl:message>
  <arr name="fieldName">
    <xsl:apply-templates select="child::node()"/>
  </arr>
</xsl:template>

<xsl:template match="luceneMatchVersion">
  <xsl:if test=".!='7.4.0'">
    <xsl:message>* Changed "luceneMatchVersion" to 7.4.0</xsl:message>
  </xsl:if>
  <luceneMatchVersion>7.4.0</luceneMatchVersion>
</xsl:template>

<xsl:template match="requestHandler[@class='solr.admin.AdminHandlers']">
  <xsl:message>* Removed deprecated solr.admin.AdminHandlers RequestHandler</xsl:message>
</xsl:template>

<xsl:template match="requestHandler[@class='org.apache.solr.handler.admin.SecureAdminHandlers']">
  <xsl:message>* Removed org.apache.solr.handler.admin.SecureAdminHandlers configuration</xsl:message>
</xsl:template>

<xsl:template match="requestHandler[@class='org.apache.solr.handler.dataimport.DataImportHandler']">
  <xsl:message>* Removed org.apache.solr.handler.dataimport.DataImportHandler configuration</xsl:message>
</xsl:template>

<xsl:template match="requestHandler[@class='solr.JsonUpdateRequestHandler']">
  <xsl:message>* Removed solr.JsonUpdateRequestHandler configuration</xsl:message>
</xsl:template>

<xsl:template match="requestHandler[@class='solr.CSVRequestHandler']">
  <xsl:message>* Removed solr.CSVRequestHandler configuration</xsl:message>
</xsl:template>

</xsl:stylesheet>
