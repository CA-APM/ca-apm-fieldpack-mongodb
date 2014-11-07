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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
// import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;

/**
 * Convenience class for setting up a MongoDB Replica set
 *
 */
public class MongoReplicaSetForTestFactory {

    private final static Logger logger =
        Logger.getLogger(MongoReplicaSetForTestFactory.class.getName());

    public static final String ADMIN_DATABASE_NAME = "admin";

    private final Map<String, List<IMongodConfig>> replicaSet;
    private final String database;

    private List<MongodProcess> mongodProcessList;

    public MongoReplicaSetForTestFactory(
        final Map<String, List<IMongodConfig>> replSet,
        final String dbName
     ) {
        this.replicaSet = replSet;
        this.database = dbName;
    }

    public void start() throws Throwable {
        this.mongodProcessList = new ArrayList<MongodProcess>();
        Set<Entry<String, List<IMongodConfig>>> entries = replicaSet.entrySet();
        for (Entry<String, List<IMongodConfig>> entry : entries) {
            initializeReplicaSet(entry);
        }
    }

    public void stop() {
        for (MongodProcess process : this.mongodProcessList) {
            process.stop();
        }
    }

    private void initializeReplicaSet(Entry<String, List<IMongodConfig>> entry
    ) throws Exception {
        String replicaName = entry.getKey();
        List<IMongodConfig> mongoConfigList = entry.getValue();

        if (mongoConfigList.size() < 3) {
            throw new Exception(
                "A replica set must contain at least 3 members.");
        }
        // Create 3 mongod processes
        for (IMongodConfig mongoConfig : mongoConfigList) {
            if (!mongoConfig.replication().getReplSetName()
                .equals(replicaName)) {
                throw new Exception(
                    "Replica set name must match in mongo configuration");
            }
            MongodStarter starter = MongodStarter.getDefaultInstance();
            MongodExecutable mongodExe = starter.prepare(mongoConfig);
            MongodProcess process = mongodExe.start();
            mongodProcessList.add(process);
        }
        Thread.sleep(1000);
        MongoClientOptions mo = MongoClientOptions.builder()
            .autoConnectRetry(true)
            .build();
        MongoClient mongo =
            new MongoClient(new ServerAddress(mongoConfigList.get(0).net()
                    .getServerAddress().getHostName(), mongoConfigList.get(0)
                    .net().getPort()), mo);
        DB mongoAdminDB = mongo.getDB(ADMIN_DATABASE_NAME);

        CommandResult cr = mongoAdminDB
            .command(new BasicDBObject("isMaster", 1));
        logger.info("isMaster: " + cr);

        // Build BSON object replica set settings
        DBObject replicaSetSetting = new BasicDBObject();
        replicaSetSetting.put("_id", replicaName);
        BasicDBList members = new BasicDBList();
        int i = 0;
        for (IMongodConfig mongoConfig : mongoConfigList) {
            DBObject host = new BasicDBObject();
            host.put("_id", i++);
            host.put("host", mongoConfig.net().getServerAddress().getHostName()
                + ":" + mongoConfig.net().getPort());
            members.add(host);
        }

        replicaSetSetting.put("members", members);
        logger.info(replicaSetSetting.toString());
        // Initialize replica set
        cr = mongoAdminDB.command(new BasicDBObject("replSetInitiate",
                replicaSetSetting));
        logger.info("replSetInitiate: " + cr);

        Thread.sleep(5000);
        cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
        logger.info("replSetGetStatus: " + cr);

        // Check replica set status before to proceed
        while (!isReplicaSetStarted(cr)) {
            logger.info("Waiting for 3 seconds...");
            Thread.sleep(1000);
            cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
            logger.info("replSetGetStatus: " + cr);
        }

        mongo.close();
        mongo = null;
    }

    private boolean isReplicaSetStarted(BasicDBObject setting) {
        if (setting.get("members") == null) {
            return false;
        }

        BasicDBList members = (BasicDBList) setting.get("members");
        for (Object m : members.toArray()) {
            BasicDBObject member = (BasicDBObject) m;
            logger.info(member.toString());
            int state = member.getInt("state");
            logger.info("state: " + state);
            // 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
            if (state != 1 && state != 2 && state != 7) {
                return false;
            }
        }
        return true;
    }
}
