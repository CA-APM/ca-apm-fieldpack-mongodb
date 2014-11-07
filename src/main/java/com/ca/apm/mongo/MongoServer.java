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

public class MongoServer {

    private String host;
    private int port;

    protected MongoServer(final String mongoSrv) throws Exception {
        if (mongoSrv != null) {
            String[] parts = mongoSrv.split(":");
            if (parts.length != 2) {
                throw new Exception(
                    String.format("Bad Host/Port found: %s", mongoSrv));
            }
            host = parts[0];
            port = Integer.parseInt(parts[1]);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
