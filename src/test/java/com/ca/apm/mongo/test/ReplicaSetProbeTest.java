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
import org.testng.SkipException;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;

import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.*;
import de.flapdoodle.embed.process.runtime.*;
import de.flapdoodle.embed.mongo.tests.*;

import com.mongodb.*;
import javax.json.Json;
import javax.json.stream.JsonParser;

import net.jadler.Jadler;

import com.ca.apm.mongo.Collector;

public class ReplicaSetProbeTest {

    private static final String RUN_REPL_TEST = "RUN_REPL_TEST";
    private static final int baseReplSetPort = 23456;
    private static final int HTTP_PORT = 9997;

    private MongoReplicaSetForTestFactory testFactory;

    private List<Properties> testPropList;

    private final String database = "TestDatabase";
    private final String replSetName = "TestReplicaSet";

    private final int numReplicaServers = 3;

    private final List<Path> dbDirs = new ArrayList<Path>();

    @BeforeClass
    public void setUp() throws Throwable {
        if (!TestUtil.configuredToRun(RUN_REPL_TEST)) {
            System.err.printf(
                "Skipping %s as it is not configured to run. Specifiy -D%s" +
                " to run this test.%n",
                this.getClass().getName(), RUN_REPL_TEST);
            return;
        }
        final Map<String, List<IMongodConfig>> replSets =
            TestUtil.createReplConfig(
                replSetName, baseReplSetPort, numReplicaServers, dbDirs);
        testFactory =
            new MongoReplicaSetForTestFactory(replSets, database);

        testFactory.start();

        if (testPropList == null) {
            testPropList = new ArrayList<Properties>();
        }
        for (IMongodConfig cfg : replSets.get(replSetName)) {
            testPropList.add(initializeProps(cfg.net().getPort()));
        }
        try {
            Thread.sleep(20 * 1000);
        } catch (Exception e) {
            // do nothing
        }
    }

    @AfterClass
    public void tearDown() throws Exception {

        if (!TestUtil.configuredToRun(RUN_REPL_TEST)) {
            return;
        }
        testFactory.stop();
        for (Path dbDir : dbDirs) {
            TestUtil.deleteDir(dbDir);
        }
    }

    @Test
    public void testJson() throws Exception {

        if (!TestUtil.configuredToRun(RUN_REPL_TEST)) {
            throw new SkipException(
                String.format("%s property not set", RUN_REPL_TEST));
        }
        for (Properties testProps : testPropList) {
                Collector collector = new Collector(testProps);
            final String host = testProps.getProperty(Collector.DB_HOST_PROP);
            final int port =
                Integer.parseInt(testProps.getProperty(Collector.DB_PORT_PROP));

            String json = collector.makeMetrics(
                collector.getMongoData(host, port)).toString();
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
    }

    @Test
    public void testEndToEnd() throws Exception {

        if (!TestUtil.configuredToRun(RUN_REPL_TEST)) {
            throw new SkipException(
                String.format("%s property not set", RUN_REPL_TEST));
        }
        for (Properties testProps : testPropList) {
            setupJadlerForEndToEnd();

            Collector.collect(testProps);

            tearDownJadlerForEndToEnd();
        }

    }

    private void setupJadlerForEndToEnd() {
        Jadler.initJadlerListeningOn(HTTP_PORT);
        Jadler.onRequest()
            .havingMethodEqualTo("POST")
            .havingPathEqualTo("/apm/metricFeed")
            .respond().withStatus(200);
    }

    private void tearDownJadlerForEndToEnd() {
        try {
            Jadler.verifyThatRequest()
                .havingMethodEqualTo("POST")
                .havingPathEqualTo("/apm/metricFeed")
                .receivedTimes(testPropList.size());
        } finally {
            Jadler.closeJadler();
        }
    }

    private Properties initializeProps(final int port) throws Exception {
        Collector.setupLogger(null);
        Properties props = new Properties();
        props.setProperty(Collector.DB_HOST_PROP,
            Network.getLocalHost().getHostName());
        props.setProperty(Collector.DB_PORT_PROP,
            String.valueOf(port));
        props.setProperty(Collector.COLLECTION_INTERVAL_PROP,
            String.valueOf(0));
        props.setProperty(Collector.APM_HOST_PROP, "localhost");
        props.setProperty(Collector.APM_PORT_PROP,
            String.valueOf(HTTP_PORT));
        props.setProperty(Collector.DB_AUTH_PROP, Collector.AUTH_NONE);

        return props;
    }
}
