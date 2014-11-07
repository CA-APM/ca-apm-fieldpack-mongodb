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
 */

package com.ca.apm.mongo.test;

import java.io.StringReader;

import javax.json.Json;
import javax.json.stream.JsonParser;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.ca.apm.mongo.MetricFeedBundle;
import com.ca.apm.mongo.MetricPath;

public class MetricTest {

    @Test
    public void testMetricPathTranslation() {
        MetricPath mp = new MetricPath("basePath");
        mp.addElement("no|pipes:or|colons");
        mp.addElement("element2");
        mp.addMetric("mname");
        Assert.assertEquals("basePath|no_pipes;or_colons|element2:mname",
            mp.toString());

        mp = new MetricPath("base");
        mp.addElement("nothing;fancy");
        mp.addElement("to_see_here");
        mp.addMetric("mname");
        Assert.assertEquals("base|nothing;fancy|to_see_here:mname",
            mp.toString());
    }

    public void testJsonEscapes() {
        MetricFeedBundle mfb = new MetricFeedBundle();
        mfb.addMetric("StringEvent", "weird\\name",
            "C:\\windows\\path\\foo.exe");
        String json = mfb.toString();
        try {
            JsonParser p = Json.createParser(new StringReader(json));
            while (p.hasNext()) {
                p.next();
            }
            p.close();
        } catch (Exception ex) {
            Assert.fail("JSON parse exception", ex);
        }
    }
}