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

/**
 *
 *
 * @author
 */
public class ClusterProbeTest {

    private static final int mongosPort = 12346;
    private static final int baseReplSetPort = 23456;
    private static final int baseCfgServerPort = 34567;

    private static final int HTTP_PORT = 9998;

    private static final String RUN_CLUSTER_TEST = "RUN_CLUSTER_TEST";

    private MongosSystemForTestFactory testFactory;

    private MongoClient mongoClient;
    private List<Properties> testPropList;

    private final String shardDB = "TestShardDB";
    private final String shardColl = "TestShardCollection";
    private final String shardKey = "TestShardKey";
    private final String replSetName = "TestShard";

    private final int numConfigServers = 3;
    private final int numReplicaServers = 3;

    private final List<Path> dbDirs = new ArrayList<Path>();

    @BeforeClass
    public void setUp() throws Throwable {

        if (!TestUtil.configuredToRun(RUN_CLUSTER_TEST)) {
            System.err.printf(
                "Skipping %s as it is not configured to run. Specifiy -D%s" +
                " to run this test.%n",
                this.getClass().getName(), RUN_CLUSTER_TEST);
            return;
        }

        final IMongosConfig shRouter = createShardRouterConfig();
        final Map<String, List<IMongodConfig>> shReplSets =
            TestUtil.createReplConfig(
                replSetName, baseReplSetPort, numReplicaServers, dbDirs);
        final List<IMongodConfig> cfgServers = createConfigSrvsConfig();

        testFactory =
            new MongosSystemForTestFactory(
                shRouter, shReplSets, cfgServers, shardDB, shardColl, shardKey);

        testFactory.start();

        if (testPropList == null) {
            testPropList = new ArrayList<Properties>();
        }
        testPropList.add(
            initializeProps(shRouter.net().getPort()));
        for (IMongodConfig cfg : shReplSets.get(replSetName)) {
            testPropList.add(initializeProps(cfg.net().getPort()));
        }
        for (IMongodConfig cfg : cfgServers) {
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

        if (!TestUtil.configuredToRun(RUN_CLUSTER_TEST)) {
            throw new SkipException(
                String.format("%s property not set", RUN_CLUSTER_TEST));
        }
        testFactory.stop();
        for (Path dbDir : dbDirs) {
            TestUtil.deleteDir(dbDir);
        }
    }

    @Test
    public void testJson() throws Exception {

        if (!TestUtil.configuredToRun(RUN_CLUSTER_TEST)) {
            throw new SkipException(
                String.format("%s property not set", RUN_CLUSTER_TEST));
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

        if (!TestUtil.configuredToRun(RUN_CLUSTER_TEST)) {
            return;
        }
        for (Properties testProps : testPropList) {
            setupJadlerForEndToEnd();

            Collector.collect(testProps);

            tearDownJadlerForEndToEnd();
        }

    }

    private IMongosConfig createShardRouterConfig(
    ) throws Exception {
        String cfgServers = getConfigServers();
        return new MongosConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(mongosPort, Network.localhostIsIPv6()))
            .configDB(cfgServers)
            .build();
    }

    private List<IMongodConfig> createConfigSrvsConfig(
    ) throws Exception {

        final List<IMongodConfig> configServers =
            new ArrayList<IMongodConfig>(numConfigServers);

        for (int i = 0; i < numConfigServers; i++) {
            configServers.add(
                new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(baseCfgServerPort + i, Network.localhostIsIPv6()))
                .configServer(true)
                .build());
        }
        return configServers;
    }

    private String getConfigServers() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numConfigServers; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(String.format("%s:%d",
                    Network.getLocalHost().getHostName(),
                    baseCfgServerPort + i));
        }
        return sb.toString();
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
