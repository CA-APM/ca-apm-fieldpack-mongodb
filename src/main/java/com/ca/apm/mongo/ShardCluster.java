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
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.bson.BasicBSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public final class ShardCluster extends Topology {

    public ShardCluster(
        final Properties props,
        final String h,
        final int p,
        final Logger l
    ) {
        super(props, h, p, l, ClusterType.SHARDED_CLUSTER);
    }

    private String host = dbHost;
    private int port = dbPort;

    public void discoverServers(final String nodeType) throws Exception {

        List<String> shardRouters = null;
        List<String> shardServers = null;
        List<String> cfgServers = null;

        logger.log(Level.FINE, "Shard Cluster Node Type: {0}", nodeType);

        if ("shardRouter".equals(nodeType)) {

            shardServers = getShardsFromMongos(host, port);
            shardRouters = getMongosFromConfig(host, port);
            cfgServers = getConfigServersFromMongos(host, port);

        } else if ("shardConfigServer".equals(nodeType)) {

            shardRouters = getMongosFromConfig(host, port);
            for (String shardRouter : shardRouters) {
                MongoServer ms = new MongoServer(shardRouter);
                cfgServers = getConfigServersFromMongos(
                    ms.getHost(), ms.getPort());
                break;
            }
            shardServers = getShardsFromConfig(host, port);

        } else if ("shardMember".equals(nodeType)) {

            cfgServers = getConfigServersFromShard(host, port);
            for (String cfgServer : cfgServers) {
                MongoServer ms = new MongoServer(cfgServer);
                shardRouters =
                    getMongosFromConfig(ms.getHost(), ms.getPort());
                break;
            }
            for (String cfgServer : cfgServers) {
                MongoServer ms = new MongoServer(cfgServer);
                shardServers =
                    getShardsFromConfig(ms.getHost(), ms.getPort());
                break;
            }

        } else {
            throw new RuntimeException("UnKnown Cluster node type!");
        }

        members.addAll(shardRouters);
        members.addAll(shardServers);
        members.addAll(cfgServers);
    }

    private List<String> getShardsFromMongos(
        final String host,
        final int port
    ) throws Exception {

        final List<String> shardResult = new ArrayList<String>();

        final CommandResult cr = dbAdminCmd(host, port, "listShards");

        if (cr.ok()) {
            final BasicDBList shardList= (BasicDBList)cr.get("shards");
            for (Object obj : shardList) {
                final BasicDBObject bdbo = (BasicDBObject) obj;
                String shards = bdbo.getString("host");
                if (shards.indexOf("/") != -1) {
                    final String [] shardMembers =
                        shards.split("/")[1].split(",");
                    for (String member : shardMembers) {
                        final MongoServer ms =
                            new MongoServer(member);
                        shardResult.addAll(discoverReplicas(
                                ms.getHost(), ms.getPort()));
                    }
                } else {
                    // single node shard
                    shardResult.add(shards);
                }
            }
        }
        return shardResult;
    }

    private List<String> getShardsFromConfig(
        final String host,
        final int port
    ) {

        final List<String> shardList = new ArrayList<String>();

        MongoClient dbClient = null;

        try {
            dbClient = setupDbClient(host, port);

            final DB configDB = dbClient.getDB("config");
            final DBCursor shardsCursor =
                configDB.getCollectionFromString("shards").find();
            while (shardsCursor.hasNext()) {
                final DBObject dbo = shardsCursor.next();
                String shards = (String) dbo.get("host");
                String [] shardMembers = getShardMembers(shards);
                for (String member : shardMembers) {
                    shardList.add(member);
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                "Exception getting shards from cfg servers: {0}", e);
        } finally {
            if (dbClient != null) {
                dbClient.close();
            }
        }
        return shardList;
    }

    private List<String> getMongosFromConfig(
        final String host,
        final int port
    ) {

        final List<String> shardRouters = new ArrayList<String>();

        MongoClient dbClient = null;

        try {
            dbClient = setupDbClient(host, port);
            final DB configDB = dbClient.getDB("config");
            final DBCursor mongosCursor =
                configDB.getCollectionFromString("mongos").find();
            while (mongosCursor.hasNext()) {
                final DBObject dbo = mongosCursor.next();
                final String mongos = (String) dbo.get("_id");
                shardRouters.add(mongos);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                "Exception getting mongos from cfg server(s): {0}", e);
        } finally {
            if (dbClient != null) {
                dbClient.close();
            }
        }
        return shardRouters;
    }

    private List<String> getConfigServersFromMongos(
        final String host,
        final int port
    ) throws Exception {
        final List<String> cfgServers = new ArrayList<String>();

        final BasicBSONObject parsed = getParsedCmdLineOpts(host, port);

        final BasicBSONObject sharding =
            (BasicBSONObject) parsed.get("sharding");
        String cfgServerString; 
        if (sharding != null)
            cfgServerString = sharding.getString("configDB");
        else
            cfgServerString = (String) parsed.get("configDB");
        addCommaSeparatedHosts(cfgServers, cfgServerString);

        return cfgServers;
    }

    private List<String> getConfigServersFromShard(
        final String host,
        final int port
    ) throws Exception {

        final List<String> cfgServers = new ArrayList<String>();

        String cHost = host;
        int cPort = port;

        final CommandResult isMaster = dbAdminCmd(cHost, cPort, "isMaster");

        // we can't run the DB command "shardingState" from a node
        // which isn't a master.  This should only apply to non-primary
        // replica members, so find the primary and run the command on it.
        if (! isMaster.getBoolean("ismaster")) {
            if (isMaster.containsField("primary")) {
                final String primary = isMaster.getString("primary");
                final MongoServer ms = new MongoServer(primary);
                cHost = ms.getHost();
                cPort = ms.getPort();
            }
        }

        CommandResult shardState =
            dbAdminCmd(cHost, cPort, "shardingState");

        if (shardState.ok()) {
            final String cfgSrvs =
                shardState.getString("configServer");
            addCommaSeparatedHosts(cfgServers, cfgSrvs);
        }
        return cfgServers;
    }

    private String[] getShardMembers(final String shardString) {
        String shards = shardString;
        if (shards.indexOf("/") != -1) {
            shards = shards.split("/")[1];
        }
        return shards.split(",");
    }

    private void addCommaSeparatedHosts(
        final Collection<String> c,
        final String hosts) {
        if (hosts != null) {
            for (String host : hosts.split(",")) {
                c.add(host);
            }
        }
    }

    private BasicBSONObject getParsedCmdLineOpts(
        final String host,
        final int port
    ) throws Exception {

        BasicBSONObject parsed = null;

        final CommandResult clOptions =
            dbAdminCmd(host, port, "getCmdLineOpts");

        if (clOptions.ok()) {
            parsed = (BasicBSONObject) clOptions.get("parsed");
        } else {
            throw new RuntimeException(
                "Failed to get command line options!");
        }
        return parsed;
    }
}
