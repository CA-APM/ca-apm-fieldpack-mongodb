//
//
// Copyright (c) 2014 CA. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// IN NO EVENT WILL CA BE LIABLE TO THE END USER OR ANY THIRD PARTY FOR ANY LOSS
// OR DAMAGE, DIRECT OR INDIRECT, FROM THE USE OF THIS MATERIAL,
// INCLUDING WITHOUT LIMITATION, LOST PROFITS, BUSINESS INTERRUPTION, GOODWILL,
// OR LOST DATA, EVEN IF CA IS EXPRESSLY ADVISED OF SUCH LOSS OR DAMAGE.
//
//
// Simple app to perform CRUD operations against a test database in mongodb.
// The test database is set in CRUDDB_NAME below.
// Simply creates, reads, updates, and deletes objects.
// Set RUN_MINS below to drive how long you want this to run.
//     The CRUD loop will run every 15 seconds until RUN_MINS have expired.
// Each CRUD loop:
//    - creates NUM_ADDS items in the crudCollection
//    - reads all the items
//    - updates half the items
//    - deletes a random number of the items
//

// Some constants to drive the app
// These may be changed prior to running to control length and amount of data.
var CRUDDB_NAME = "crudDB";
var RUN_MINS = 2;
var NUM_ADDS = 100;

//
// Get a random number
//
// Returns a random number between min (inclusive) and max (exclusive)
function getRandom(min, max) {
    return Math.random() * (max - min) + min;
}

//
// Create some items in the collection
//
function createData() {
    print("Creating " + NUM_ADDS);

    var collCnt = db.crudCollection.count();
    for (var i = 0; i < NUM_ADDS; i++) {
        var idx = collCnt + i;
        db.crudCollection.insert(
            {
             createTime: new Date()
            }
        );
    }
}

//
// Simply find/read all the items
//
function readData() {
    var collCnt = db.crudCollection.count();

    print("Reading " + collCnt);

    var cursor = db.crudCollection.find();
    while (cursor.hasNext()) {
        var doc = cursor.next();
        var tmp = doc._id;
        //printjson(doc);
    }
}

//
// Update half the items.
//
function updateData() {
    var collCnt = db.crudCollection.count();
    var opCnt = Math.round(collCnt/2);

    print("Updating " + opCnt);

    var cursor = db.crudCollection.find().limit(opCnt);
    while (cursor.hasNext()) {
        var doc = cursor.next();
        db.crudCollection.update(
            { _id: {$in: [doc._id]} },
            { $currentDate: {updateTime:  true} }
        );
    }
}

//
// Delete some of the items.
//
function deleteData() {
    var collCnt = db.crudCollection.count();
    var opCnt = Math.round(getRandom(1, collCnt)/5);

    print("Deleting " + opCnt);
    var doc;
    for (var i = 0; i < opCnt; i++) {
        doc = db.crudCollection.findOne();
        db.crudCollection.remove(doc);
    }
}

function printCount() {
    print("Collection count: " + db.crudCollection.count());
}

function sleep(milliseconds) {
    var start = new Date().getTime();
    while (true) {
        if ((new Date().getTime() - start) > milliseconds) {
            break;
        }
    }
}

function doCrudOps() {
    var runTimeMillis = RUN_MINS * 60 * 1000;
    var start = new Date().getTime();

    while (true) {
        var newDate = new Date();
        if ((newDate.getTime() - start) < runTimeMillis){
            print("");
            print("CRUD Cycle @" + newDate);
            printCount();

            // CRUD ops
            createData();
            readData();
            updateData();
            deleteData();

            printCount();

            sleep(15000);
        } else {
            break;
        }
    }
}

//
// connect to mongo instance running on localhost on the default port
//
var conn = new Mongo();
var db = conn.getDB(CRUDDB_NAME);
print("");
print("Connecting to: " + db);

// drop the collection and start fresh on every run.
print("Dropping crudCollection");
var dropped = db.crudCollection.drop();

// do the CRUD operations
doCrudOps();
