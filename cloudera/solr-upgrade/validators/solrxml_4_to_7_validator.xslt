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

<xsl:template match="str[@name='adminHandler'][text()='org.apache.solr.handler.admin.SecureAdminHandlers']">
  <incompatibility>
    <level>error</level>
    <jira_number>SENTRY-1475</jira_number>
    <description>org.apache.solr.handler.admin.SecureAdminHandlers class is removed</description>
    <recommendation>Use org.apache.solr.handler.admin.CoreAdminHandler class instead</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
</xsl:template>

<xsl:template match="str[@name='adminHandler'][text()='org.apache.solr.handler.admin.SecureCoreAdminHandler']">
  <incompatibility>
    <level>error</level>
    <jira_number>SENTRY-1475</jira_number>
    <description>org.apache.solr.handler.admin.SecureCoreAdminHandler class is removed</description>
    <recommendation>Use org.apache.solr.handler.admin.CoreAdminHandler class instead</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
</xsl:template>

<xsl:template match="str[@name='collectionsHandler'][text()='org.apache.solr.handler.admin.SecureCollectionsHandler']">
  <incompatibility>
    <level>error</level>
    <jira_number>SENTRY-1475</jira_number>
    <description>org.apache.solr.handler.admin.SecureCollectionsHandler class is removed</description>
    <recommendation>Use org.apache.solr.handler.admin.CollectionsHandler class instead</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
</xsl:template>

<xsl:template match="str[@name='configSetsHandler'][text()='org.apache.solr.handler.admin.SecureConfigSetsHandler']">
  <incompatibility>
    <level>error</level>
    <jira_number>SENTRY-1475</jira_number>
    <description>org.apache.solr.handler.admin.SecureConfigSetsHandler class is removed</description>
    <recommendation>Use org.apache.solr.handler.admin.ConfigSetsHandler class instead</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
</xsl:template>

<xsl:template match="str[@name='infoHandler'][text()='org.apache.solr.handler.admin.SecureInfoHandler']">
  <incompatibility>
    <level>error</level>
    <jira_number>SENTRY-1475</jira_number>
    <description>org.apache.solr.handler.admin.SecureInfoHandler class is removed</description>
    <recommendation>Use org.apache.solr.handler.admin.InfoHandler class instead</recommendation>
    <reindexing>no</reindexing>
    <transform>no</transform>
  </incompatibility>
</xsl:template>


</xsl:stylesheet>
