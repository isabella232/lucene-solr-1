# A quick start for the docker based upgrade tests
Run the tests with the following command or from your IDE:

`mvn clean install -Psolr-dependencies-available-in-maven,\!skip-tests`

The purpose of the profile adjustment is that tests are completely turned off by default.
This is needed because docker is not available on all slave machines and there is an 
unresolved dependency problem otherwise on cauldron machines. See [dependency section](#dependency-on-solr-artifacts-in-maven)
at the end.

At first run it will build a docker image that will take long. Later on this can be turned off (see below).

## Dependencies
- Docker installed and running
	> On Linux docker daemon needs to be opened on 2375 port. This can be achieved by the following command before the docker daemon startup.
	
	>	`echo '{ "hosts":["tcp://0.0.0.0:2375"] }' >>/etc/docker/daemon.json`
- Get solr...tar.gz built using ant package from the lucene-solr/solr directory
- Copy the solr tgz from lucene-solr/solr/package/ or create a sym link in the resources/solr/dockerfiles/[solr_all | solr5localcopy]
- Solr version numbers in files
  > `resourced/solr/dockerfiles/solr_all`
  > `../pom.xml`

## Running and debugging

- After the docker images are built locally, you can add `-DskipImageBuilding` to the test run. That will skip this step and use the previously built image.
- Should you need to debug a solr issue in a docker image you can add `-DskipCleanup`. That will leave the docker processes running. You can then use `docker ps -a` to list those running processes. And then use `docker exec -it runSolrCloudOnRestartedZookeeper1 bash` to launch an interactive shell in that docker context.
- If you want to start upgrade starting from different starting CDH5 version, you can do it like the following `-Ddocker_source_version=5.13.0`
- Network interface may be stuck
  > In such cases you may forcefully clean up network interface.
  > `docker network disconnect --force bridge runPreviousAndThenCurrentSolr2`
  
## Dependency on Solr artifacts in maven

