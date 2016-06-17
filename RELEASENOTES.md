# CA APM MONGODB COLLECTOR

1. [DESCRIPTION](#description)
1. [PLATFORMS, PREREQUISITES, AND DEPENDENCIES](#platforms-prerequisites-and-dependencies)
1. [CHANGELOG](#changelog)
1. [INSTALLATION](#installation)
1. [CONFIGURATION](#configuration)
1. [CONTRIBUTING AND DEVELOPMENT](#contributing-and-development)
1. [LICENSE](#license)

## DESCRIPTION
The CA APM MongoDB Collector (hereafter Collector) extracts performance metrics from one or more MongoDB instances and send them to an APM Enterprise Manager via EPAgent.  It utilizes the RESTful API for EPAgent.

In addition to the Collector program, this bundle also contains example dashboards for display of MongoDB metrics in CA APM.  Screenshots can be found on the [repo wiki](https://github.com/CA-APM/ca-apm-fieldpack-mongodb/wiki).

The Collector can run on any machine (java enabled).  It makes client connections to configured MongoDB instances and retrieves all metrics available via [serverStatus](http://docs.mongodb.org/manual/reference/command/serverStatus/).  Metrics are then forwarded to a CA APM EPAgent via RESTful API.

## PLATFORMS, PREREQUISITES, AND DEPENDENCIES

This extension requires the following:

- All CA APM versions should work.  There are no version dependencies.

- CA APM EPAgent v9.7.1 or later (with REST interface).  The EPAgent can be running on any machine (provided the configured HTTP port is reachable by the collector).

- The ca-apm-fieldpack-mongodb program is self-contained except for dependencies on
   - [Java MongoDB Driver](http://docs.mongodb.org/ecosystem/drivers/java/)
   - [google-gson](https://code.google.com/p/google-gson/)
  
- MongoDB versions supporting [serverStatus](http://docs.mongodb.org/manual/reference/command/serverStatus/) should work.  The Collector was built, tested, and certified against MongoDB v2.6.
 
## CHANGELOG

Please review the **CHANGELOG.md** file in this repository

## INSTALLATION

Unzip ca-apm-fieldpack-mongodb-1.0.zip into a directory with suitable read/write access. [Configure](#configuration) your setup.

### Running the Collector

With an appropriate property file and the dependent jars, run the
program as follows:

`java -cp ca-apm-fieldpack-mongodb-1.0.jar:mongo-java-driver-2.12.3.jar:gson-2.2.4.jar
com.ca.apm.mongo.Collector <your-propfile>.props`

### Example Dashboard Setup

In order to view the example dashboards, you need to install a management module
and a calculator on a CA APM Enterprise Manager. To install:

* copy ./dashboard/MongoDB_MgtModule.jar <EM install path>/config/modules
* copy ./dashboard/MongoDBMemoryLeak.js <EM install path>/scripts

## CONFIGURATION

All configuration of the Collector program is via property file -- **example.props** for details.

## CONTRIBUTING AND DEVELOPMENT

If you plan to contribute changes, you need to read the **CONTRIBUTING.md** file in this repository for details on how to collaborate.

Development details follow:

### Builds

The repo includes a **pom.xml** for building via maven. A typical maven
build would be:
`mvn clean package`

This will resolve external dependencies, build source and test
classes, run tests with code coverage and yield an output zip. 

### External Dependencies

The external dependencies for the implementation are:

* [Java MongoDB Driver](http://docs.mongodb.org/ecosystem/drivers/java/).
* [google-gson](https://code.google.com/p/google-gson/)

The tests depend on:

* the TestNG framework
* an [embedded mongo library](https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo)
* a json parser [javax.json](https://jsonp.java.net/)
* a [mock http server](https://github.com/jadler-mocking/jadler/wiki)

All dependencies are resolved by maven.

### Unit Tests

This repo includes two varieties of unit test. One uses the embedded
mongo that was packaged specifically for ease of unit testing. The other
tests use an enterprise mongo instance since various configuration
options such as SSL are only available in the enterprise edition.
Because there is no easy/portable way to make enterprise mongo
available, these tests will only run if a property named MONGO_PROGRAM
is set like so:

`mvn -DMONGO_PROGRAM=/tmp/enterprise_mongod clean test`

The tests for clustered, replicated and sharded mongo instances use the embedded
mongo, but they consume a significant amount of disk space (~30GB) so
they too are only enabled by property like so:

    mvn -DRUN_REPL_TEST clean test # Runs the ReplicaSetProbeTest suite
    mvn -DRUN_CLUSTER_TEST clean test # Runs the ClusterProbeTest suite


There is a test case for LDAP which was made to work on a local
developer desktop, but since significant setup is required outside of
mongo this test is not enabled by default. See the References section
for the documents used in setting up the LDAP test.

### Code Coverage

Jacoco code coverage reports are built in to the build. Results can
be viewed in `./target/site/jacoco-ut/index.html`

### References

* [MongoDB Manual](http://docs.mongodb.org/manual/)
  * [Mongo Monitoring Best Practices](https://www.mongodb.com/partners/partner-program/technology/certification/m$
  * [Metric Producing Command](http://docs.mongodb.org/manual/reference/command/serverStatus)
  * [Java Client](http://api.mongodb.org/java/2.12)
  * [SSL Configuration](http://docs.mongodb.org/manual/tutorial/configure-ssl)
  * [Authentication](http://docs.mongodb.org/manual/tutorial/enable-authentication)
  * [LDAP Configuration](http://docs.mongodb.org/manual/tutorial/configure-ldap-sasl-openldap)
    * [CentOS Setup for Testing](http://www.itmanx.com/kb/centos6/install-openldap-phpldapadmin)

## LICENSE

Please review the **LICENSE** file in this repository.
