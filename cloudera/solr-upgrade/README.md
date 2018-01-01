# Solr config upgrade tool #

Despite widespread enterprise adoption, Solr lacks automated upgrade tooling. It has long been a challenge for users to understand the implications of a Solr upgrade. Users must manually review the Solr release notes to identify configuration changes either to fix backwards incompatibilities or to utilize latest features in the new version. Additionally, users must identify a way to migrate existing index data to the new version (either via an index upgrade or re-indexing the raw data).

Solr config upgrade tool aims to simplify the upgrade process by providing upgrade instructions tailored to your configuration. These instuctions can help you to answer following questions

- Does my Solr configuration have any backwards incompatible sections? If yes which ones?
- For each of the incompatibility - what do I need to do to fix this incompatibility? Where can I get more information about why this incompatibility was introduced (e.g. references to Lucene/Solr jiras)?
- Are there any changes in Lucene/Solr which would require me to do a full reindexing OR can I get away with an index upgrade?

 ## High level design ##

This tool is built using [Extensible Stylesheet Language Transformations](https://en.wikipedia.org/wiki/XSLT) engine. The upgrade rules, implemented in the form of XSLT transformations, can identify backwards incompatibilities and in some cases can even fix them automatically.

 In general, an incompatibility can be categorized as follows,
 - An incompatibility due to removal of Lucene/Solr configuration element (e.g. a field type) is marked as ERROR in the validation result. Typically this will result in failure to start the Solr server (or load the core). User must make changes to Solr configuration using application specific knowledge to fix such incompatibility.
 - An incompatibility due to deprecation of a configuration section in the new Solr version is marked as WARNING in the validation result. Typically this will not result in any failure during Solr server startup (or core loading), but may prevent application from utilizing new Lucene/Solr features (or bug-fixes). User may choose to make changes to Solr configuration using application specific knowledge to fix such incompatibility.
 - An incompatibility which can be fixed automatically (e.g. by rewriting the Solr configuration section) and do not require any manual intervention is marked as INFO in the validation result. This also includes incompatibilities in the underlying Lucene implementation (e.g. [LUCENE-6058](https://issues.apache.org/jira/browse/LUCENE-6058)) which would require rebuilding the index (instead of index upgrade). Typically such incompatibility will not result in failure during Solr server startup (or core loading), but may affect the accuracy of the query results or consistency of underlying indexed data.

## Steps to build this tool ##
- Requires JDK 8 +
- Build this project using maven (mvn clean package)
- The distribution of this tool is available under target folder as a .tar.gz file.

## Steps to run this tool ##

- Requires Java 8 at minimum
- While this tool is available as part of Cloudera Search (version 6), it is also included as part of Cloudera Manager (version 6) to simplify and streamline Solr upgrade process. When this tool is run from Cloudera Manager distribution, you will need to configure the location of Solr binaries available as part of the CDH5 deployment using CDH_SOLR_HOME environment variable. e.g. in case of parcel based deployments, Solr binaries are available under /opt/cloudera/parcels/CDH/lib/solr directory.
```bash
export CDH_SOLR_HOME=/opt/cloudera/parcels/CDH/lib/solr
```
- Run the config upgrade tool as follows
```bash
./solr-upgrade.sh help

Usage: ./solr-upgrade.sh command [command-arg]
Options:
    --zk   zk_ensemble
    --debug Prints error output of calls
    --trace Prints executed commands
Commands:
  help
  download-metadata -d dest_dir
  validate-metadata -c metadata_dir
  restore-metadata -c metadata_dir
  config-upgrade [--dry-run] -c conf_path -t conf_type -u upgrade_processor_conf -d result_dir [-v]
Parameters:
  -c <arg>     This parameter specifies the path of Solr configuration to be operated upon.
  -t <arg>     This parameter specifies the type of Solr configuration to be validated and
               transformed.The currently accepted values are schema, solrconfig and solrxml.
  -d <arg>     This parameter specifies the directory path where the result of the command
               should be stored.
  -u <arg>     This parameter specifies the path of the Solr upgrade processor configuration.
  --dry-run    This command will perform compatibility checks for the specified Solr configuration.
  -v           This parameter enables printing XSLT compiler warnings on the command output.
```

# Solr upgrade process #
Following steps are recommended during the Solr upgrade process. This tool will help in performing various task related to configuration backup, migration as well as restore. Please note that the migration of Lucene index files is not in scope for this tool. For index migration, it is recommended to perform re-indexing after the upgrade is complete (Ref: https://wiki.apache.org/solr/HowToReindex)

## steps to perform before the upgrade ##

### Backup your Solr configuration and data
Before proceeding with an upgrade, you need to backup your Solr collection. This will allow you to rollback to the pre-upgrade state if something goes wrong during the upgrade process. If you are managing Solr using Cloudera Manager, then this step is not needed since Cloudera Manager will backup the Solr configuration and data automatically.

### Install Solr config upgrade tool
The Solr config upgrade tool is available as part of Cloudera Manager (version 6). Hence you should upgrade your Cloudera Manager instance to the latest version (version 6+). The tool is available at location /opt/cloudera/cm/solr-upgrade.

### Downloading the metadata
We need to migrate the SOLR metadata stored in Zookeeper before the upgrade. The upgrade tool provides "download-metadata" command for this purpose. This command downloads important configuration files in SOLR e.g. solr.xml, clusterstate.json, collection configsets etc. to a specified location on local file-system. e.g. following command downloads the Solr metadata to /backups/solr directory.

```bash
./solr-upgrade.sh download-metadata -d /backups/solr
Cleaning up /backups/solr
Copying clusterstate.json
Copying clusterprops.json
Copying solr.xml
Downloading config named books_config for collection books
Successfully downloaded SOLR metadata in Zookeeper
```
In case the cluster (specifically Zookeeper) is configured in a secure mode, then additional parameters are required to configure Zookeeper client accordingly. Please use ZKCLI_JVM_FLAGS environment variable for this purpose. The typical parameters to configure are jaas configuration file and Solr Zookeeper ACL provider. e.g. following command configures the tool to backup metadata for a CDH5 version of Solr,

```bash
export ZKCLI_JVM_FLAGS='-Djava.security.auth.login.config=/path/to/jaas.conf -DzkACLProvider=org.apache.solr.common.cloud.ConfigAwareSaslZkACLProvider'
```

### Migrating the configuration
Once the Solr metadata is downloaded, run the migration tool to convert the configuration files to be compatible with the version of Solr being upgraded to. The tool supports migrating Solr schema.xml (and managed-schema), solrconfig.xml as well as solr.xml configuration files. e.g. following command runs the upgrade tool on a Solr schema.xml of version 4.x to identify incomatibilities before upgrade to Solr 7

```bash
./solr-upgrade.sh config-upgrade -t schema -c schema.xml -u validators/solr_4_to_7_processors.xml -d /tmp
Validating schema...

Following configuration errors found:

      * Legacy field type (name = pint and class = solr.IntField) is removed.
      * Legacy field type (name = plong and class = solr.LongField) is removed.
      * Legacy field type (name = pfloat and class = solr.FloatField) is removed.
      * Legacy field type (name = pdouble and class = solr.DoubleField) is removed.
      * Legacy field type (name = pdate and class = solr.DateField) is removed.
      * Legacy field type (name = sint and class = solr.SortableIntField) is removed.

No configuration warnings found...

Please note that in Solr 7:
    * Users of the BeiderMorseFilterFactory will need to rebuild their indexes after upgrading

Solr schema validation failed. Please review /tmp/schema_validation.xml for more details.
```

### Validating the migrated configuration
During the CDH upgrade, Cloudera Manager will run a validation check against the migrated SOLR metadata to ensure that the user has performed the configuration migration and the migrated metadata can be used to re-initialize SOLR service post upgrade. This command perform series of checks to ensure (a) important configuration files are present in the metadata folder e.g. solr.xml, clusterstate.json, collection configs etc. (b) the configuration files are compatible with the version of SOLR being upgraded to. e.g. following command validates the metadata stored in /backups/solr directory

```bash
./solr-upgrade.sh validate-metadata -c /backups/solr
Validating metadata in /backups/solr
validating solr configuration using config upgrade processor @ ./validators/solr_4_to_7_processors.xml
---------- validating solr.xml ----------
Validating solrxml...
No configuration errors found...
No configuration warnings found...

Solr solrxml validation is successful. 

---------- validation successful for solr.xml ----------
---------- validating configset books_config ----------
Validating solrconfig...
No configuration errors found...
No configuration warnings found...

Solr solrconfig validation is successful.

Validating schema...

Following configuration errors found:

      * Legacy field type (name = pint and class = solr.IntField) is removed.
      * Legacy field type (name = plong and class = solr.LongField) is removed.
      * Legacy field type (name = pfloat and class = solr.FloatField) is removed.
      * Legacy field type (name = pdouble and class = solr.DoubleField) is removed.
      * Legacy field type (name = pdate and class = solr.DateField) is removed.

No configuration warnings found...

Please note that in Solr 7:
    * The implicit default Similarity is changed to SchemaSimilarityFactory

Solr schema validation failed.
```

## steps to perform after the upgrade ##

### Bootstrap SOLR configuration
Once the CDH upgrade is complete, global metadata (e.g. solr.xml) compatible with the latest SOLR version needs to be configured before SOLR service can be started successfully. Following command can be used for this purpose,

```bash
./solr-upgrade.sh bootstrap-config -c /backups/solr
```

### Restart SOLR service
Restart SOLR service so that SOLR can start using latest configuration metadata uploaded as part of the last step.

### Recreate collections in SOLR
TBD
