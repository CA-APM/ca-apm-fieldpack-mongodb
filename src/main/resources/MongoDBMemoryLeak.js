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
// Sample MongoDB Calculator.
// If metric value for mem:virtual value is significantly larger than 
// for mem:mapped (e.g. 3 or more times), this may indicate a memory leak.
// So return 
//    Math.round( (virtual/mapped) * 100)
// in the metric mem:VirtualMappedMemoryLeakRatio
// in order to allow the dashboard to show something about this relationship
//
function execute(metricData,javascriptResultSetHelper) 
{
    var i=0;
    var mongoInstanceNames = {};
    var virtualMems = {};
    var mappedMems = {};
    
    for(i=0; i < metricData.length; i++) {
        var metric = metricData[i].agentMetric.attributeURL;
        var agent = metricData[i].agentName.processURL;
        var value = metricData[i].timeslicedValue.value;
        var frequency = metricData[i].frequency;
        var isAbsent = metricData[i].timeslicedValue.dataIsAbsent();

        //log.info("MongoDB calculator: metric = " + metric + " = " + value);

        if (((metric.indexOf("mem:mapped") > 0) ||
            (metric.indexOf("mem:virtual") > 0)) &&
            (isAbsent == false)) {

            var BTTopLevel = metric.substring(0, metric.indexOf(":")); // top level path w/o metric name
            var fullBTTree = agent +"|"+ BTTopLevel;                // full path with agent name
            //log.info("MongoDB calculator: fullBTTree = " + fullBTTree);
            mongoInstanceNames[fullBTTree] = fullBTTree;

            if (metric.indexOf("mem:mapped") > 0) {
                mappedMems[fullBTTree] = value;
            }
            if (metric.indexOf("mem:virtual") > 0) {
                virtualMems[fullBTTree] = value;
            }
        }
    }
    
    // now iterate found metrics and report calculated metrics
    for (var name in mongoInstanceNames) {
    
        var memComparisonMetricName = 
            name
            + ":VirtualMappedMemoryLeakIndicatorRatio";

        var mapped = mappedMems[name];
        var virtual = virtualMems[name];
        var ratio = 0; // report as 0 if missing metrics or divide by 0

        //log.info("MongoDB calculator: mapped = " + mapped);
        //log.info("MongoDB calculator: virtual = " + virtual);

        if (mapped != undefined && 
            virtual != undefined &&
            mapped > 0) {

            var ratio = virtual / mapped;
            //log.info("MongoDB calculator: ratioValue = " +  memComparisonMetricName + " = " + ratio);
        }

        // doing *100 since we can only report integers back and not doubles
        // and we don't want to lose that much precision on the ratio
        var reportedValue = Math.round(ratio * 100);
        //log.info("MongoDB calculator: ratioValue = " +  memComparisonMetricName + " = " + reportedValue);

        javascriptResultSetHelper.addMetric(
            memComparisonMetricName,
            reportedValue,
            javascriptResultSetHelper.kIntegerConstant,
            javascriptResultSetHelper.kDefaultFrequency)
    }
    
    // return the result set
    return javascriptResultSetHelper;
}

// Tell the EM what Agents we should match against
function getAgentRegex() 
{
    return ".*";
}

// The the EM what metrics we should match against
function getMetricRegex() 
{
    return "MongoDB@.*mem.*";
}

// must return a multiple of default system frequency (currently 15 seconds)
function getFrequency() 
{
    return 15;
}

// Return false if the script should not run on the MOM.
// Scripts that create metrics on agents other than the Custom Metric Agent
// should not run on the MOM because the agents exist only in the Collectors.
// Default is true.
function runOnMOM() 
{
    return false;
}
