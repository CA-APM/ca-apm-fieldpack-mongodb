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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.logging.Level;

import javax.net.ssl.SSLSocketFactory;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;


public abstract class Topology {

    protected enum ClusterType {
        STANDALONE("Standalone Mongod"),
        REPLICA_SET("Replica Set"),
        SHARDED_CLUSTER("Sharded Cluster");

        private String name;

        private ClusterType(String inName) {
            name = inName;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    protected Logger logger;

    private Properties props;
    private List<MongoCredential> mongoCreds = new ArrayList<MongoCredential>();
    protected String dbHost;
    protected int dbPort;

    protected ClusterType type;

    protected Set<String> members = new HashSet<String>();

    protected Topology(
        final Properties props,
        final String host,
        final int port,
        final Logger l,
        final ClusterType t
    ) {
        this.props = props;
        Collector.setupCreds(mongoCreds, props);
        setConnProps(host, port);
        setLogger(l);
        type = t;
    }

    public void setConnProps(final String host, final int port) {
        this.dbHost = host;
        this.dbPort = port;
    }

    public void setLogger(final Logger logger) {
        this.logger = logger;
    }

    abstract void discoverServers(final String nodeType) throws Exception;

    public List<String> getDiscoveredServers() {
        return new ArrayList<String>(members);
    }

    protected List<String> discoverReplicas(final String host, final int port)
        throws Exception {

        List<String> replicas = new ArrayList<String>();

        final CommandResult cr = dbAdminCmd(host, port, "isMaster");

        replicas.addAll((List<String>) cr.get("hosts"));
        // hidden replicas are replicas that cannot become primaries and
        // are hidden to the client app
        if (cr.getBoolean("hidden")) {
            // TODO: We can't assume we're the master here....
            replicas.add(cr.getString("me"));
        }
        // passives are replicas that cannot become primaries because
        // their priority is set to 0
        if (cr.containsField("passives")) {
            replicas.addAll((List<String>) cr.get("passives"));
        }
        // arbiters exist only to vote in master elections.  They don't
        // actually hold replica data.
        if (cr.containsField("arbiters")) {
            replicas.addAll((List<String>) cr.get("arbiters"));
        }
        return replicas;
    }

    protected CommandResult dbAdminCmd(
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

    protected MongoClient setupDbClient(final String dbHost, final int dbPort) {
        final boolean useSSL =
            Collector.getBooleanProp(Collector.USE_SSL_PROP, props);
        final String clientTrustStore =
            Collector.getOptionalStringProp(
                Collector.SSL_CLIENT_TRUST_STORE_FILE_PROP, props);
        final String clientPasswd =
            Collector.getOptionalStringProp(
                Collector.SSL_CLIENT_TRUST_STORE_PASSWD_PROP, props);

        try {
            MongoClientOptions.Builder builder =
                new MongoClientOptions.Builder();

            if (useSSL) {
                System.setProperty(
                    Collector.SSL_CLIENT_TRUST_STORE_FILE_PROP,
                    clientTrustStore);
                System.setProperty(
                    Collector.SSL_CLIENT_TRUST_STORE_PASSWD_PROP,
                    clientPasswd);
                builder = builder.socketFactory(SSLSocketFactory.getDefault());
            }

            final MongoClientOptions options = builder.build();
            MongoClient dbClient = new MongoClient(
                new ServerAddress(dbHost, dbPort),
                mongoCreds,
                options);
            logger.log(Level.FINE, "Connected to mongo at {0}",
                dbClient.getConnectPoint());
            logger.log(Level.FINE, "Client options: "
                + dbClient.getMongoClientOptions());
            return dbClient;
        } catch (Exception ex) {
            throw new RuntimeException(
                "Can't initialize mongo client", ex);
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("%s%n", type));
        for (String member : members) {
            sb.append("  Server: ");
            sb.append(member);
            sb.append("\n");
        }
        return sb.toString();
    }
}
