<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

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

<xsl:output method="html" version="1.0" encoding="UTF-8" indent="true"/>

<xsl:param name="solrOpVersion" />
<xsl:param name="dryRun" />

<xsl:template match="result">
<html>
  <body>
    <h2>Please note the terminology in this report</h2>
    <ul>
       <li>An incompatibility due to removal of Lucene/Solr configuration element (e.g. a field type) is marked as ERROR in the validation result.
       Typically this will result in failure to start the Solr server (or load the core). You must make changes to Solr configuration using application
       specific knowledge to fix such incompatibility.</li>
       <li>An incompatibility due to deprecation of a configuration section in the new Solr version is marked as WARNING in the validation result.
       Typically this will not result in any failure during Solr server startup (or core loading), but may prevent application from utilizing new Lucene/Solr
       features (or bug-fixes). You may choose to make changes to Solr configuration using application specific knowledge to fix such incompatibility.</li>
       <li>An incompatibility which can be fixed automatically (e.g. by rewriting the Solr configuration section) and do not require any manual intervention
       is marked as INFO in the validation result. This also includes incompatibilities in the underlying Lucene implementation which would require rebuilding
       the index (instead of index upgrade). Typically such incompatibility will not result in failure during Solr server startup (or core loading), but may
       affect the accuracy of the query results or consistency of underlying indexed data.</li>
    </ul>
    <xsl:choose>
      <xsl:when test="/result/incompatibility[contains(level, 'error')]">
        <h2>Following configuration errors found:</h2>
        <table border="1">
          <tr bgcolor="#9acd32">
            <th>Description</th>
            <th>Recommendation</th>
            <th>Lucene/Solr JIRAs</th>
            <th>Requires re-indexing</th>
          </tr>
          <xsl:for-each select="/result/incompatibility[contains(level, 'error')]">
            <tr>
              <td><xsl:value-of select="./description"/></td>
              <td><xsl:value-of select="./recommendation"/></td>
              <td><xsl:value-of select="./jira_number"/></td>
              <td><xsl:value-of select="./reindexing"/></td>
            </tr>
          </xsl:for-each>
        </table>
        <br />
      </xsl:when>
      <xsl:otherwise><h2>No configuration errors found</h2></xsl:otherwise>
    </xsl:choose>
    <xsl:choose>
      <xsl:when test="/result/incompatibility[contains(level, 'warn')]">
        <h2>Following configuration warnings found:</h2>
        <table border="1">
          <tr bgcolor="#9acd32">
            <th>Description</th>
            <th>Recommendation</th>
            <th>Lucene/Solr JIRAs</th>
            <th>Requires re-indexing</th>
          </tr>
          <xsl:for-each select="/result/incompatibility[contains(level, 'warn')]">
            <tr>
              <td><xsl:value-of select="./description"/></td>
              <td><xsl:value-of select="./recommendation"/></td>
              <td><xsl:value-of select="./jira_number"/></td>
              <td><xsl:value-of select="./reindexing"/></td>
            </tr>
          </xsl:for-each>
        </table>
        <br />
      </xsl:when>
      <xsl:otherwise><h2>No configuration warnings found</h2></xsl:otherwise>
    </xsl:choose>
    <xsl:choose>
      <xsl:when test="/result/incompatibility[contains(level, 'info') and contains(transform, 'yes')]">
        <h2>Following incompatibilities will be fixed by auto-transformations (using --upgrade command):</h2>
        <table border="1">
          <tr bgcolor="#9acd32">
            <th>Description</th>
            <th>Recommendation</th>
            <th>Lucene/Solr JIRAs</th>
            <th>Requires re-indexing</th>
          </tr>
          <xsl:for-each select="/result/incompatibility[contains(level, 'info') and contains(transform, 'yes')]">
            <tr>
              <td><xsl:value-of select="./description"/></td>
              <td><xsl:value-of select="./recommendation"/></td>
              <td><xsl:value-of select="./jira_number"/></td>
              <td><xsl:value-of select="./reindexing"/></td>
            </tr>
          </xsl:for-each>
        </table>
        <br />
      </xsl:when>
    </xsl:choose>
    <xsl:choose>
      <xsl:when test="/result/incompatibility[contains(level, 'info') and contains(transform, 'no')]">
        <h2>Please note the other incompatibilities in Solr <xsl:value-of select="$solrOpVersion"/>:</h2>
        <table border="1">
          <tr bgcolor="#9acd32">
            <th>Description</th>
            <th>Recommendation</th>
            <th>Lucene/Solr JIRAs</th>
            <th>Requires re-indexing</th>
          </tr>
          <xsl:for-each select="/result/incompatibility[contains(level, 'info') and contains(transform, 'no')]">
            <tr>
              <td><xsl:value-of select="./description"/></td>
              <td><xsl:value-of select="./recommendation"/></td>
              <td><xsl:value-of select="./jira_number"/></td>
              <td><xsl:value-of select="./reindexing"/></td>
            </tr>
          </xsl:for-each>
        </table>
        <br />
      </xsl:when>
    </xsl:choose>
  </body>
</html>
</xsl:template>
</xsl:stylesheet>