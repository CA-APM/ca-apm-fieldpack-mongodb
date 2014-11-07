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

import java.util.Properties;

import java.util.logging.Logger;

public class ReplicaSet extends Topology {

    protected ReplicaSet(
        final Properties props,
        final String h,
        final int p,
        final Logger l
    ) {
        super(props, h, p, l, ClusterType.REPLICA_SET);
    }

    private String host = dbHost;
    private int port = dbPort;

    public void discoverServers(final String nodeType) throws Exception {
        members.addAll(discoverReplicas(host, port));
    }
}
