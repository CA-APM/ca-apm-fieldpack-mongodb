/*
 *
 * Copyright (c) 2014 CA. All rights reserved.  
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 
 * IN NO EVENT WILL CA BE LIABLE TO THE END USER OR ANY THIRD PARTY FOR ANY LOSS
 * OR DAMAGE, DIRECT OR INDIRECT, FROM THE USE OF THIS MATERIAL,
 * INCLUDING WITHOUT LIMITATION, LOST PROFITS, BUSINESS INTERRUPTION, GOODWILL,
 * OR LOST DATA, EVEN IF CA IS EXPRESSLY ADVISED OF SUCH LOSS OR DAMAGE.
 * 
 */

package com.ca.apm.mongo.test;

import static org.testng.Assert.*;
import org.testng.annotations.*;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;

import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.*;
import de.flapdoodle.embed.process.runtime.*;

import com.mongodb.*;
import javax.json.Json;
import javax.json.stream.JsonParser;

import javax.net.ssl.SSLSocketFactory;

import net.jadler.Jadler;

import com.ca.apm.mongo.Collector;

/**
 *
 *
 * @author
 * @version $Revision: 1.1.1.1 $
 */
public class ProbeTest {
    private MongodStarter mongoStarter;
    private MongodExecutable mongoExe;
    private MongodProcess mongod;
    private MongoClient mongoClient;
    private Properties testProps;
    private Process mongoProcess;
    private Path mongoDirPath;

    private static final int MONGO_PORT = 12345;
    private static final int HTTP_PORT = 9999;
    private static final String MONGO_PROGRAM = "MONGO_PROGRAM";
    private static final String SSL_DIR = "ssl";
    private static final String defaultUser = "apmMonitor";

    @Test
    public void testNoAuth() throws Exception {
        setupNoAuth();
        testInvalidProps();
        initializeProps();
        testJson();
        testEndToEnd();
        tearDownNoAuth();
    }

    @Test
    public void testCRAuth() throws Exception {
        final String mongoProgram = System.getProperty(MONGO_PROGRAM);
        if (mongoProgram == null) {
            System.err.printf(
                "MONGO_PROGRAM not specified; skipping CRAuth test%n");
            return;
        }
        setupCRAuth(mongoProgram);
        try {
            testProps.setProperty(Collector.DB_USER_PROP, defaultUser);
            testProps.setProperty(Collector.DB_PASSWD_PROP, defaultUser);
            testProps.setProperty(Collector.DB_AUTH_PROP, Collector.AUTH_CR);
            testEndToEnd();
            assertNoMetricsWhenUnauthorized();
        } finally {
            shutdownExternalMongo();
        }
    }

    private String sslDirRelative(final String tmpDir, final String filename) {
        return tmpDir + File.separatorChar + SSL_DIR
            + File.separatorChar + filename;
    }

    private void copySSLFiles(final String tmpDir) {
        final Path source =
            new File("target" + File.separatorChar + "test-classes"
                + File.separatorChar + SSL_DIR).toPath();
        final Path target = new File(tmpDir).toPath().resolve(SSL_DIR);

        try {
            Files.copy(source, target, StandardCopyOption.COPY_ATTRIBUTES);

            Files.walkFileTree(
                source,
                new SimpleFileVisitor<Path>() {
                    @Override
                        public FileVisitResult visitFile(
                            final Path file,
                            final BasicFileAttributes attrs
                        ) throws IOException {
                        Files.copy(file,
                            target.resolve(source.relativize(file)));
                        return FileVisitResult.CONTINUE;
                    }
                });
        } catch (final Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void setupCRAuth(final String mongo) throws Exception {
        final List<String> params = new ArrayList<String>();
        params.add("--auth");
        startExternalMongo(mongo, params);
        createMongoUser(defaultUser, false, false);
    }

    private void startExternalMongo(final String mongo) throws Exception {
        startExternalMongo(mongo, new ArrayList<String>());
    }

    private void startExternalMongo(
        final String mongo,
        final List<String> params
    ) throws Exception {
        mongoDirPath = Files.createTempDirectory("mongo_probe_test");
        final String tmpDir = mongoDirPath.toString();
        copySSLFiles(tmpDir);

        final List<String> cmd =
            new ArrayList<String>(
                Arrays.asList(
                    mongo,
                    "--dbpath", tmpDir.toString(),
                    "--sslMode", "allowSSL",
                    "--sslPEMKeyFile", sslDirRelative(tmpDir, "mongodb.pem"),
                    "--sslPEMKeyPassword", "mongod",
                    "--sslCAFile", sslDirRelative(tmpDir, "client.pem"),
                    "--sslWeakCertificateValidation",
                    "--noprealloc", "--smallfiles", "--nojournal",
                    "--nohttpinterface", "--syncdelay=0",
                    "--port", String.valueOf(MONGO_PORT)));
        cmd.addAll(params);

        final ProcessBuilder pb = new ProcessBuilder(cmd);
        mongoProcess = pb.start();
        initializeProps();
    }

    private void shutdownExternalMongo() {
        mongoProcess.destroy();
        mongoProcess = null;
        deleteMongoDir();
    }

    private void deleteMongoDir() {
        if (mongoDirPath == null) {
            return;
        }
        try {
            TestUtil.deleteDir(mongoDirPath);
        } catch (Exception ex) {
            System.err.printf("can't delete mongo dir: %s%n", mongoDirPath);
        } finally {
            mongoDirPath = null;
        }
    }

    private void setupNoAuth() throws Exception {
        mongoStarter = MongodStarter.getDefaultInstance();
        mongoExe = mongoStarter.prepare(
            new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(MONGO_PORT, Network.localhostIsIPv6()))
            .build());
        mongod = mongoExe.start();
    }

    private void tearDownNoAuth() throws Exception {
        mongod.stop();
        mongoExe.stop();
    }

    private void testInvalidProps() throws Exception {
        assertBadProperty(Collector.DB_PORT_PROP, "not numeric");
        assertBadProperty(Collector.DB_HOST_PROP, null);
        assertBadProperty(Collector.DB_PORT_PROP, null);
        assertBadProperty(Collector.COLLECTION_INTERVAL_PROP, "not num");
        assertBadProperty(Collector.APM_HOST_PROP, null);
        assertBadProperty(Collector.APM_PORT_PROP, null);
        assertBadProperty(Collector.APM_PORT_PROP, "not num");
        assertBadProperty(Collector.DB_AUTH_PROP, null);
        assertBadProperty(Collector.DB_AUTH_PROP, "bogus");
    }

    private void testJson() throws Exception {
        Collector collector = new Collector(testProps);
        final String host = testProps.getProperty(Collector.DB_HOST_PROP);
        final int port =
            Integer.parseInt(testProps.getProperty(Collector.DB_PORT_PROP));

        String json = collector.makeMetrics(
            collector.getMongoData(host, port)).toString();
        // assert that JSON parses successfully...
        try {
            JsonParser p = Json.createParser(new StringReader(json));
            while (p.hasNext()) {
                p.next();
            }
            p.close();
        } catch (Exception ex) {
            fail("JSON parse exception", ex);
        }
    }

    private void testEndToEnd() throws Exception {
        setupJadlerForEndToEnd();
        Collector.collect(testProps);
        Collector.collect(testProps);
        tearDownJadlerForEndToEnd();
    }

    private void assertNoMetricsWhenUnauthorized() throws Exception {
        assertNoMetricsWhenUnauthorized(testProps);
    }

    private void assertNoMetricsWhenUnauthorized(
        final Properties p
    ) throws Exception {
        p.setProperty(Collector.DB_USER_PROP, "baduser");
        p.setProperty(Collector.DB_PASSWD_PROP, "badpasswd");
        Jadler.initJadlerListeningOn(HTTP_PORT);
        Jadler.onRequest()
            .havingMethodEqualTo("POST")
            .havingPathEqualTo("/apm/metricFeed")
            .respond().withStatus(200);

        Collector.collect(p);

        Jadler.verifyThatRequest()
            .havingMethodEqualTo("POST")
            .havingPathEqualTo("/apm/metricFeed")
            .receivedTimes(0);
        Jadler.closeJadler();
    }

    @Test
    public void testSSL() throws Exception {
        final String mongoProgram = System.getProperty(MONGO_PROGRAM);
        if (mongoProgram == null) {
            System.err.printf(
                "MONGO_PROGRAM not specified; skipping SSL test%n");
            return;
        }

        startExternalMongo(mongoProgram);
        try {
            final Properties sslProps = (Properties) testProps.clone();
            addSSLProps(sslProps);
            setupJadlerForEndToEnd();
            Collector.collect(sslProps);
            Collector.collect(sslProps);
            tearDownJadlerForEndToEnd();
        } finally {
            shutdownExternalMongo();
        }
    }

    // @Test
    public void testX509() throws Exception {
        final String mongoProgram = System.getProperty(MONGO_PROGRAM);
        if (mongoProgram == null) {
            System.err.printf(
                "MONGO_PROGRAM not specified; skipping X509 test%n");
            return;
        }
        String x509user = "emailAddress=goat@bobspants.org,CN=client,OU=COS,O=CA,L=Ouray,ST=CO,C=US";

        startExternalMongo(mongoProgram);
        try {
            createMongoUser(x509user, true, true);
            final Properties x509Props = (Properties) testProps.clone();
            addSSLProps(x509Props);
            x509Props.setProperty(Collector.DB_USER_PROP, x509user);
            x509Props.setProperty(Collector.DB_AUTH_PROP, Collector.AUTH_X509);
            setupJadlerForEndToEnd();
            Collector.collect(x509Props);
            Collector.collect(x509Props);
            tearDownJadlerForEndToEnd();
        } finally {
            shutdownExternalMongo();
        }
    }

    // @Test
    // public void testKerberos() throws Exception {
    //     final String mongoProgram = System.getProperty(MONGO_PROGRAM);
    //     if (mongoProgram == null) {
    //         System.err.printf(
    //             "MONGO_PROGRAM not specified; skipping Kerberos test%n");
    //         return;
    //     }

    //     startExternalMongo(mongoProgram);
    //     // XXX: need to create user here
    //     final Properties sslProps = (Properties) testProps.clone();
    //     addSSLProps(sslProps);
    //     assertNoMetricsWhenUnauthorized(sslProps);
    //     setupJadlerForEndToEnd();
    //     Collector.collect(sslProps);
    //     Collector.collect(sslProps);
    //     tearDownJadlerForEndToEnd();
    //     shutdownExternalMongo();
    // }

    private void setupLdapMongo(
        final String mongo
    ) throws Exception {
        final List<String> cmd = new ArrayList<String>();
        cmd.add("--auth");
        cmd.add("--setParameter");
        cmd.add("saslauthdPath=/var/run/saslauthd/mux");
        cmd.add("--setParameter");
        cmd.add("authenticationMechanisms=PLAIN");
        startExternalMongo(mongo, cmd);
    }

    // @Test
    // Test is not enabled because it relies on configuration of LDAP
    // which may not be present on all machines
    public void testLdap() throws Exception {
        final String mongoProgram = System.getProperty(MONGO_PROGRAM);
        if (mongoProgram == null) {
            System.err.printf(
                "MONGO_PROGRAM not specified; skipping LDAP test%n");
            return;
        }
        setupLdapMongo(mongoProgram);
        try {
            createMongoUser("raul", true, true);
            final Properties props = (Properties)testProps.clone();
            addSSLProps(props);
            props.setProperty(Collector.DB_AUTH_PROP, Collector.AUTH_SASL);
            assertNoMetricsWhenUnauthorized(props);

            setupJadlerForEndToEnd();
            props.setProperty(Collector.DB_USER_PROP, "raul");
            props.setProperty(Collector.DB_PASSWD_PROP, "RoyalStreet");
            Collector.collect(props);
            Collector.collect(props);
            tearDownJadlerForEndToEnd();
        } finally {
            shutdownExternalMongo();
        }
    }

    private void setupJadlerForEndToEnd() {
        Jadler.initJadlerListeningOn(HTTP_PORT);
        Jadler.onRequest()
            .havingMethodEqualTo("POST")
            .havingPathEqualTo("/apm/metricFeed")
            .respond().withStatus(200)
            .thenRespond().withStatus(409).withBody(
                "log detail for bad request");
    }

    private void tearDownJadlerForEndToEnd() {
        Jadler.verifyThatRequest()
            .havingMethodEqualTo("POST")
            .havingPathEqualTo("/apm/metricFeed")
            .receivedTimes(2);
        Jadler.closeJadler();
    }

    private void addSSLProps(final Properties props) {
        final String sourceDir =
            "target" + File.separatorChar + "test-classes"
            + File.separatorChar + SSL_DIR;
        props.setProperty(Collector.USE_SSL_PROP, "true");
        props.setProperty(Collector.SSL_CLIENT_TRUST_STORE_FILE_PROP,
            sourceDir + File.separatorChar + "clientTruststore");
        props.setProperty(Collector.SSL_CLIENT_TRUST_STORE_PASSWD_PROP,
            "mongod");
    }

    private void createMongoUser(
        final String mongoUser,
        final boolean useSSL,
        final boolean useExternalAuth
    ) throws Exception {
        final String sourceDir =
            "target" + File.separatorChar + "test-classes"
            + File.separatorChar + SSL_DIR;

        MongoClientOptions.Builder builder =
            new MongoClientOptions.Builder();

        if (useSSL) {
            System.setProperty(Collector.SSL_CLIENT_TRUST_STORE_FILE_PROP,
                sourceDir + File.separatorChar + "clientTruststore");
            System.setProperty(Collector.SSL_CLIENT_TRUST_STORE_PASSWD_PROP,
                "mongod");
            builder = builder.socketFactory(SSLSocketFactory.getDefault());
        }

        final MongoClientOptions options = builder.build();
        final MongoClient client = new MongoClient(
            new ServerAddress("localhost", MONGO_PORT), options);
        final BasicDBList roles = new BasicDBList();
        final BasicDBObject role = new BasicDBObject("role", "clusterMonitor");
        role.put("db", "admin");
        roles.add(role);
        final BasicDBObject user = new BasicDBObject("createUser", mongoUser);
        user.put("roles", roles);
        DB db = client.getDB("admin");
        if (useExternalAuth) {
            db = db.getSisterDB("$external");
        } else {
            user.put("pwd", mongoUser);
        }
        final CommandResult cr = db.command(user);
        if (!successfulCommand(cr)) {
            throw new RuntimeException("can't create user: " + cr);
        }
        System.out.printf("Created user: %s external: %s%n",
            mongoUser, useExternalAuth);
    }

    private boolean successfulCommand(final CommandResult cr) {
        Object ok = cr.get("ok");
        Object errmsg = cr.get("errmsg");
        if (errmsg != null) {
            System.err.printf("Command error: %s%n", errmsg);
        }
        return ok != null && errmsg == null && ((Number)ok).intValue() == 1;
    }

    private void initializeProps() {
        Collector.setupLogger(null);
        testProps = new Properties();
        testProps.setProperty(Collector.DB_HOST_PROP, "localhost");
        testProps.setProperty(Collector.DB_PORT_PROP,
            String.valueOf(MONGO_PORT));
        testProps.setProperty(Collector.COLLECTION_INTERVAL_PROP,
            String.valueOf(0));
        testProps.setProperty(Collector.APM_HOST_PROP, "localhost");
        testProps.setProperty(Collector.APM_PORT_PROP,
            String.valueOf(HTTP_PORT));
        testProps.setProperty(Collector.DB_AUTH_PROP, Collector.AUTH_NONE);
    }

    private void assertBadProperty(
        final String nm,
        final String val
    ) {
        try {
            String exstr;
            Properties badProps = (Properties)testProps.clone();
            if (val == null) {
                badProps.remove(nm);
                exstr = String.format("no exception for missing %s", nm);
            } else {
                badProps.setProperty(nm, val);
                exstr = String.format("no exception for invalid %s: (%s)",
                    nm, val);
            }
            Collector c = new Collector(badProps);
            fail(exstr);
        } catch (Exception ex) {
            // ok
        }
    }
}
