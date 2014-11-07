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

package com.ca.apm.mongo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import javax.net.ssl.SSLSocketFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import com.ca.apm.mongo.Topology.ClusterType;

/**
 *
 *
 * @author
 * @version $Revision: 1.1.1.1 $
 */
public class Collector implements Runnable {
    public static final String DB_HOST_PROP   = "mongo.hostname";
    public static final String DB_PORT_PROP   = "mongo.port";
    public static final String DB_USER_PROP   = "mongo.user";
    public static final String DB_PASSWD_PROP = "mongo.pw";
    public static final String DB_AUTH_PROP   = "mongo.auth";

    public static final String COLLECTION_INTERVAL_PROP =
        "mongo.interval.seconds";
    public static final String USE_SSL_PROP = "mongo.usessl";
    public static final String USE_KERB_PROP = "mongo.usekerberos";
    public static final String SSL_CLIENT_TRUST_STORE_FILE_PROP =
        "javax.net.ssl.trustStore";
    public static final String SSL_CLIENT_TRUST_STORE_PASSWD_PROP =
        "javax.net.ssl.trustStorePassword";
    public static final String APM_HOST_PROP = "apm.apihost";
    public static final String APM_PORT_PROP = "apm.apiport";

    public static final String AUTH_NONE     = "none";
    public static final String AUTH_CR       = "basic";
    public static final String AUTH_X509     = "x.509";
    public static final String AUTH_KERBEROS = "kerberos";
    public static final String AUTH_SASL     = "plainsasl";

    private static Logger logger;

    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: Collector your-propfile");
            System.exit(1);
        }

        try {
            setupLogger(new FileInputStream(args[0]));
            Properties p = new Properties();
            p.load(new FileReader(args[0]));
            logger.log(Level.INFO, "APM MongoDB Collector version: {0}",
                Collector.class.getPackage().getImplementationVersion());
            collect(p);

            for (Handler h : logger.getHandlers()){
                h.close();
            }
        } catch (Exception ex) {
            if (logger != null) {
                logger.log(Level.WARNING, "Exception: {0}",  ex);
            } else {
                System.err.println("Exception: " + ex);
            }
            System.exit(1);
        }
    }

    public static void setupLogger(FileInputStream propFile) {
        LogManager manager = LogManager.getLogManager();

        try {
            if (propFile != null){
                manager.readConfiguration(propFile);
            }
        } catch (IOException e) {
            System.err.println("Error in setting up logger: " + e);
        }

        logger = Logger.getLogger(Collector.class.getName());
        logger.setUseParentHandlers(false);
        logger.addHandler(new ConsoleHandler());

        try {
            logger.addHandler(new FileHandler());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error adding FileHandler");
        }
    }

    public static void collect(final Properties p) {
        try {
            Collector c = new Collector(p);
            if (c.collectionInterval > 0) {
                ScheduledExecutorService ses =
                    new ScheduledThreadPoolExecutor(1);
                ses.scheduleAtFixedRate(c, 0,
                    c.collectionInterval, TimeUnit.SECONDS);
                // run forever
                synchronized(ses) {
                    try {
                        ses.wait();
                    } catch (InterruptedException ie) {
                    }
                }
            } else {
                c.run();
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Exception: ", ex);
        }
    }

    private Properties props;
    private int collectionInterval;
    private boolean keepRunning;
    private URL apiUrl;
    private List<MongoCredential> mongoCreds = new ArrayList<MongoCredential>();
    private Topology topology;

    public Collector(final Properties inProps) {
        props = inProps;
        processProperties();
        try {
            topology = discoverTopology();
        } catch (Exception e) {
            logger.log(Level.WARNING,
                "Exception discovering topology: ", e);
            try {
                // just assume standalone
                final String host = getStringProp(DB_HOST_PROP);
                final int port = getIntProp(DB_PORT_PROP);
                topology = new StandaloneMongod(props, host, port, logger);
                topology.discoverServers("Standalone");
            } catch (Exception e2) {
                // for standalone server discovery, there's not really
                // anything to fail...
            }
        }
        keepRunning = true;
    }

    public void run() {
        logger.log(Level.INFO, "harvesting metrics...");
        for (String mongoSrv : topology.getDiscoveredServers()) {
            MongoServer ms = null;
            try {
                ms = new MongoServer(mongoSrv);
                CommandResult mcr =
                    getMongoData(ms.getHost(), ms.getPort());
                if (isValidData(mcr)) {
                    MetricFeedBundle mfb = makeMetrics(mcr);
                    deliverMetrics(mfb);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception: ", e);
            }
        }
    }

    public CommandResult getMongoData(
        final String host,
        final int port
    ) throws Exception {
        return dbAdminCmd(host, port, "serverStatus");
    }

    private CommandResult dbAdminCmd(
        final String host,
        final int port,
        final String cmd
    ) throws Exception {
        return runDBCmd(host, port, "admin", cmd);
    }

    private CommandResult runDBCmd(
        final String host,
        final int port,
        final String database,
        final String cmd
    ) throws Exception {
        MongoClient dbClient = null;
        try {
            dbClient = setupDbClient(host, port);
            DB db = dbClient.getDB(database);
            return db.command(cmd);
        } finally {
            if (dbClient != null) {
                dbClient.close();
            }
        }
    }

    public MetricFeedBundle makeMetrics(
        final CommandResult mcr
    ) throws Exception {
        MetricFeedBundle mfb = new MetricFeedBundle();
        ServerAddress sa = mcr.getServerUsed();
        // Add a "mongo segment" to the metric path to insure that
        // mongo metrics are grouped/segregated in the metric browser.
        // Note that we can't use ":" in that segment though
        final String basePath = String.format("MongoDB@%s;%d",
            sa.getHost(), sa.getPort());
        makeMetrics(mfb, basePath, mcr);
        return mfb;
    }

    private boolean isValidData(BasicDBObject bdo) {
        Object ok = bdo.get("ok");
        Object error = bdo.get("errmsg");
        if (ok != null && ((Number)ok).intValue() == 1 && error == null) {
            bdo.removeField("ok");
            return true;
        }
        if (error != null) {
            logger.log(Level.WARNING, "Error from mongo command: {0}", error);
        } else {
            logger.log(Level.WARNING,
                "Invalid/unexpected mongo command output: {0}", bdo);
        }
        return false;
    }

    private void makeMetrics(
        final MetricFeedBundle mfb,
        final String basePath,
        final BasicDBObject bdo
    ) throws Exception {
        for (String s : bdo.keySet()) {
            final MetricPath metricPath = new MetricPath(basePath);
            final Object o = bdo.get(s);
            if (o instanceof BasicDBObject) {
                metricPath.addElement(s);
                makeMetrics(mfb, metricPath.toString(), (BasicDBObject)o);
            } else if (o instanceof BasicDBList) {
                metricPath.addElement(s);
                processBasicDBList(mfb, metricPath.toString(), (BasicDBList)o);
            } else if (isKnownDataType(o)) {
                metricPath.addMetric(s);
                makeMetric(metricPath.toString(), o, mfb);
            } else {
                logger.log(Level.WARNING,
                    "Unknown type in mongo output for key {0}: {1}",
                    new Object[] {s, o.getClass().getName()});
            }
        }
    }

    private void processBasicDBList(
        final MetricFeedBundle mfb,
        final String basePath,
        final BasicDBList bdl
    ) throws Exception {
        int i = 0;
        for (Object o : bdl) {
            MetricPath metricPath = new MetricPath(basePath);
            if (o instanceof BasicDBObject) {
                metricPath.addElement(String.format("%d", i++));
                makeMetrics(
                    mfb, metricPath.toString(), (BasicDBObject)o);
            } else if (o instanceof BasicDBList) {
                metricPath.addElement(String.format("%d", i++));
                processBasicDBList(mfb, metricPath.toString(), (BasicDBList)o);
            } else if (isKnownDataType(o)) {
                metricPath.addMetric(String.format("%d", i++));
                makeMetric(metricPath.toString(), o, mfb);
            } else {
                logger.log(Level.WARNING,
                    "Unknown type in mongo output for DBList {0}: {1}",
                    new Object[] {bdl, o.getClass().getName()});
            }
        }
    }

    private void makeMetric(
        final String metricPath,
        final Object dataObj,
        final MetricFeedBundle mfb
    ) {
        if (dataObj instanceof String) {
            mfb.addMetric("StringEvent", metricPath, (String)dataObj);
        } else if (dataObj instanceof Number) {
            String type;
            if (dataObj instanceof Double) {
                // API doesn't support floating-point metric values
                // so we round the value to a long, and also create a string
                // metric to display the value (just for debugging etc.)
                type = "LongCounter";
                long val = Math.round((Double)dataObj);
                mfb.addMetric("LongCounter", metricPath + " (rounded)",
                    String.valueOf(val));
                mfb.addMetric("StringEvent", metricPath + " (string)",
                    dataObj.toString());
            } else {
                if (dataObj instanceof Long) {
                    type = "LongCounter";
                } else {
                    // treat as Integer
                    type = "IntCounter";
                }
                mfb.addMetric(type, metricPath, dataObj.toString());
            }
        } else if (dataObj instanceof Date) {
            mfb.addMetric("TimeStamp", metricPath,
                String.valueOf(((Date)dataObj).getTime()));
        } else if (dataObj instanceof Boolean) {
            mfb.addMetric("StringEvent", metricPath, dataObj.toString());
        }
    }

    private boolean isKnownDataType(final Object dataObj) {
        return (dataObj instanceof String ||
            dataObj instanceof Number ||
            dataObj instanceof Date ||
            dataObj instanceof Boolean);
    }

    public void deliverMetrics(
        final MetricFeedBundle mfb
    ) throws Exception {
        final String json = mfb.toString();
        final HttpURLConnection conn =
            (HttpURLConnection) apiUrl.openConnection();
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.getOutputStream().write(json.getBytes());
        final int rc = conn.getResponseCode();
        if (rc != 200) {
            logger.log(Level.SEVERE, "Error code: {0}, payload: {1}",
                new Object[] {rc, getPayload(conn.getErrorStream())});
        } else {
            logger.log(Level.INFO, "Successful metric delivery");
        }
    }

    private String getPayload(
        final InputStream is
    ) throws Exception {
        BufferedReader rdr = new BufferedReader(
            new InputStreamReader(is));
        String line;
        StringBuilder sb = new StringBuilder();
        while ((line = rdr.readLine()) != null) {
            sb.append(String.format("%s%n", line));
        }
        rdr.close();
        return sb.toString();
    }

    private void processProperties() {
        // Don't validate mongo connection since the DB may not
        // be running when we start.  But, host and port properties must
        // be set at least
        getStringProp(DB_HOST_PROP);
        getIntProp(DB_PORT_PROP);
        setupCreds(mongoCreds, props);
        setInterval();
        setApiUrl();
    }

    public static void setupCreds(
        final List<MongoCredential> mc,
        final Properties iprops) {
        final String type = getStringProp(DB_AUTH_PROP, iprops);
        String user;
        String pw;
        if (AUTH_NONE.equalsIgnoreCase(type)) {
            // nothing to do
        } else if (AUTH_CR.equalsIgnoreCase(type)) {
            user = getStringProp(DB_USER_PROP, iprops);
            pw = getStringProp(DB_PASSWD_PROP, iprops);
            mc.add(MongoCredential.createMongoCRCredential(
                    user, "admin", pw.toCharArray()));
        } else if (AUTH_X509.equalsIgnoreCase(type)) {
            user = getStringProp(DB_USER_PROP, iprops);
            System.out.printf("X509 cred user(%s)%n", user);
            mc.add(MongoCredential.createMongoX509Credential(user));
        } else if (AUTH_KERBEROS.equalsIgnoreCase(type)) {
            user = getStringProp(DB_USER_PROP, iprops);
            mc.add(MongoCredential.createGSSAPICredential(user));
        } else if (AUTH_SASL.equalsIgnoreCase(type)) {
            user = getStringProp(DB_USER_PROP, iprops);
            pw = getStringProp(DB_PASSWD_PROP, iprops);
            mc.add(MongoCredential.createPlainCredential(
                    user, "$external", pw.toCharArray()));
        } else {
            throw new IllegalArgumentException(
                String.format("Invalid %s property", DB_AUTH_PROP));
        }
    }

    private MongoClient setupDbClient(final String dbHost, final int dbPort) {
        final boolean useSSL = getBooleanProp(USE_SSL_PROP);
        final String clientTrustStore =
            getOptionalStringProp(SSL_CLIENT_TRUST_STORE_FILE_PROP);
        final String clientPasswd =
            getOptionalStringProp(SSL_CLIENT_TRUST_STORE_PASSWD_PROP);

        try {
            MongoClientOptions.Builder builder =
                new MongoClientOptions.Builder();

            if (useSSL) {
                System.setProperty(SSL_CLIENT_TRUST_STORE_FILE_PROP,
                    clientTrustStore);
                System.setProperty(SSL_CLIENT_TRUST_STORE_PASSWD_PROP,
                    clientPasswd);
                builder = builder.socketFactory(SSLSocketFactory.getDefault());
            }

            final MongoClientOptions options = builder.build();
            return new MongoClient(
                new ServerAddress(dbHost, dbPort),
                mongoCreds,
                options);
        } catch (Exception ex) {
            throw new RuntimeException(
                "Can't initialize mongo client", ex);
        }
    }

    private void setInterval() {
        collectionInterval = getIntProp(COLLECTION_INTERVAL_PROP);
    }

    private void setApiUrl() {
        final String apiHost = getStringProp(APM_HOST_PROP);
        final int apiPort = getIntProp(APM_PORT_PROP);
        try {
            apiUrl = new URL(String.format("http://%s:%d/apm/metricFeed",
                    apiHost, apiPort));
        } catch (Exception ex) {
            throw new RuntimeException("Can't initialize APM API URL", ex);
        }
    }

    private String getStringProp(final String pname) {
        return getStringProp(pname, props);
    }

    public static String getStringProp(final String pname, final Properties p) {
        final String ret = p.getProperty(pname);
        if (isEmpty(ret)) {
            throw new IllegalArgumentException(
                String.format("missing or invalid property: %s%n",
                    pname));
        }
        return ret;
    }

    private String getOptionalStringProp(final String pname) {
        return getOptionalStringProp(pname, props);
    }

    public static String getOptionalStringProp(
        final String pname,
        final Properties p
    ) {
        final String ret = p.getProperty(pname);
        if (isEmpty(ret)) {
            return "";
        }
        return ret;
    }

    private int getIntProp(final String pname) {
        int ret = 0;
        try {
            ret = Integer.parseInt(getStringProp(pname));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(
                String.format("missing or invalid integer property: %s%n",
                    pname));
        }
        return ret;
    }

    private boolean getBooleanProp(final String pname) {
        return getBooleanProp(pname, props);
    }

    public static boolean getBooleanProp(
        final String pname, final Properties p) {
        return Boolean.valueOf(p.getProperty(pname));
    }

    private static boolean isEmpty(final String s) {
        return (s == null || "".equals(s.trim()));
    }

    public Topology discoverTopology() throws Exception {

        final String host = getStringProp(DB_HOST_PROP);
        final int port = getIntProp(DB_PORT_PROP);

        logger.log(Level.INFO, "Discovering Topology for host: {0}:{1}",
            new Object[] {host, port});

        final CommandResult master = dbAdminCmd(host, port, "isMaster");

        // ismaster returns true for a standalone mongod instance, a mongos
        // instance, a mongod shard node, or a primary in a replica set
        if (master.getBoolean("ismaster") || master.containsField("primary")) {

            boolean isReplicaSet = false;
            if (master.containsField("primary")) {
                isReplicaSet = true;
            }

            if (isInShardCluster(master)) {
                topology = new ShardCluster(props, host, port, logger);
                topology.discoverServers(getClusterNodeType());
            } else if (isReplicaSet(master)) {
                topology = new ReplicaSet(props, host, port, logger);
                topology.discoverServers("doesn't matter");
            } else {
                topology = new StandaloneMongod(props, host, port, logger);
                topology.discoverServers("doesn't matter");
            }
        }
        logger.log(Level.INFO, "Topology: {0}", topology);
        return topology;
    }

    private boolean isInShardCluster(
        final CommandResult cr
    ) throws Exception {

        MongoServer ms = new MongoServer(getMyself(cr));
        final String host = ms.getHost();
        final int port = ms.getPort();

        boolean sharded = false;

        final String msg = cr.getString("msg");
        if ((msg != null) && msg.contains("isdbgrid")) {
            sharded = true;
        } else if (cr.getBoolean("ismaster")) {
            final CommandResult shardState =
                dbAdminCmd(host, port, "shardingState");
            // shardingState command only returns OK when server is in a sharded
            // cluster
            if (shardState.ok()) {
                if (shardState.getBoolean("enabled")) {
                    sharded = true;
                }
            }
        }  else if (cr.containsField("primary")) {
            // we are in a replica set but not the primary,
            // check the primary to see if it is a shard member
            final String primary = cr.getString("primary");
            ms = new MongoServer(primary);
            final CommandResult priIsMaster =
                dbAdminCmd(ms.getHost(), ms.getPort(), "isMaster");
            sharded = isInShardCluster(priIsMaster);
        }
        return sharded;
    }

    private String getClusterNodeType() throws Exception {

        final String host = getStringProp(DB_HOST_PROP);
        final int port = getIntProp(DB_PORT_PROP);

        String nodeType = null;

        final CommandResult isMaster = dbAdminCmd(host, port, "isMaster");
        if (isMaster.getBoolean("ismaster")) {
            final String msg = isMaster.getString("msg");
            if (msg != null && msg.contains("isdbgrid")) {
                nodeType = "shardRouter";
            } else {
                final CommandResult shardState =
                    dbAdminCmd(host, port, "shardingState");
                if (shardState.ok() && shardState.getBoolean("enabled")) {
                    if (isConfigServer(host, port)) {
                        nodeType = "shardConfigServer";
                    } else {
                        nodeType = "shardMember";
                    }
                }
            }
        } else if (isReplicaMember(isMaster)) {
            nodeType = "shardMember";
        }

        return nodeType;
    }

    final boolean isConfigServer(final String host, final int port) {

        boolean isConfigServer = false;

        MongoClient dbClient = null;

        try {
            dbClient = setupDbClient(host, port);
            final DB configDB = dbClient.getDB("config");
            if (configDB.getCollectionFromString("mongos").find().hasNext()) {
                isConfigServer = true;
            }
        } finally {
            if (dbClient != null) {
                dbClient.close();
            }
        }
        return isConfigServer;
    }

    private boolean isReplicaSet(final CommandResult cr) {
        return cr.containsField("primary");
    }

    /**
     * This method is to check to see if a node is in a replica set despite
     * not being the primary member.
     */
    private boolean isReplicaMember(final CommandResult cr) {
        boolean isReplMember = false;
        if (cr.containsField("primary")) {
            if (cr.getBoolean("secondary") ||
                cr.getBoolean("passive") ||
                cr.getBoolean("arbiterOnly")) {
                isReplMember = true;
            }
        }
        return isReplMember;
    }

    private String getMyself(final CommandResult cr) {
        return String.format("%s:%d",
            cr.getServerUsed().getHost(), cr.getServerUsed().getPort());
    }
}
