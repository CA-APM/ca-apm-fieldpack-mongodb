/*
 * Copyright (c) 2014 CA.  All rights reserved.
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
 * @author Dann Church (chuda04)
 */
package com.ca.apm.mongo.test;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.config.Timeout;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class TestUtil {

    protected static boolean configuredToRun(final String runTestProp) {
        final String shouldRun = System.getProperty(runTestProp);
        if (shouldRun == null) {
            return false;
        }
        return true;
    }

    public static File createTempDirectory() throws IOException{
        final File temp;
        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if(!(temp.delete())){
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if(!(temp.mkdir())){
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return temp;
    }
    protected static Map<String, List<IMongodConfig>> createReplConfig(
            final String replSetName,
            final int baseReplSetPort,
            final int numReplicaServers,
            final List<Path> dbDirs
    ) throws Exception {

        final int opLogSize = 10000;

        final Map<String, List<IMongodConfig>> replMap =
                new HashMap<String, List<IMongodConfig>>(1);

        final List<IMongodConfig> replicas =
                new ArrayList<IMongodConfig>(numReplicaServers);
        for (int i = 0; i < numReplicaServers; i++) {

            final Path dbDir = createTempDirectory().toPath();
            replicas.add(
                    new MongodConfigBuilder()
                            .version(Version.Main.PRODUCTION)
                            .net(new Net(baseReplSetPort + i, Network.localhostIsIPv6()))
                            .timeout(new Timeout(50 * 60 * 1000))
                            .replication(
                                    new Storage(dbDir.toString(), replSetName, opLogSize))
                            .configServer(false)
                            .pidFile(dbDir.resolve("mongodb.pid").toString())
                            .build());
            dbDirs.add(dbDir);
        }
        replMap.put(replSetName, replicas);
        return replMap;
    }

    protected static void deleteDir(final Path dbDir
    ) throws IOException {
        System.out.printf("Removing directory %s%n", dbDir);
        try {
            Files.walkFileTree(dbDir, new DeletingFileVisitor());
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static class DeletingFileVisitor extends SimpleFileVisitor<Path> {

        public FileVisitResult visitFile(
                Path file,
                BasicFileAttributes attrs
        ) throws IOException {
            if (attrs.isRegularFile()) {
                Files.delete(file);
            }
            return FileVisitResult.CONTINUE;
        }
        public FileVisitResult visitFileFailed(
                final Path file,
                final IOException exc) {
            System.err.printf("Something went wrong while working on: %s%n",
                    file.getFileName());
            exc.printStackTrace();
            return FileVisitResult.CONTINUE;
        }
        public FileVisitResult postVisitDirectory(
                Path dir,
                IOException e
        ) throws IOException {
            if (e == null) {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            } else {
                throw e;
            }
        }
    }
}

