# A quick start for the docker based upgrade tests
Run the tests with the usual
`mvn clean install` command, or from your IDE.

At first run it will build a docker image that will take long. Later on this can be turned off (see below).

## Dependencies
- Docker installed and running
	> On Linux docker daemon needs to be opened on 2375 port. This can be achieved by the following command before the docker daemon startup.
	>	`echo '{ "hosts":["tcp://0.0.0.0:2375"] }' >>/etc/docker/daemon.json`
- Get solr...tar.gz built using ant package from the lucene-solr/solr directory

## Running and debugging

- After the docker images are built locally, you can add `-DskipImageBuilding` to the test run. That will skip this step and use the previously built image.
- Should you need to debug a solr issue in a docker image you can add `-DskipCleanup`. That will leave the docker processes running. You can then use `docker ps -a` to list those running processes. And then use `docker exec -it runSolrCloudOnRestartedZookeeper1 bash` to launch an interactive shell in that docker context.
- If you want to start upgrade starting from different starting CDH5 version, you can do it like the following `-Ddocker_source_version=5.13.0`
- Unfortunately at the moment there is some instability in the docker framework that we could not yet eliminate.
  > Typical error is `org.apache.http.conn.UnsupportedSchemeException: http protocol is not supported`
  > In such cases kill and remove remaining docker containers and restart your tests.