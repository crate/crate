/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.common.collections.Lists2;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;

@IntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 2, supportsDedicatedMasters = false)
public class NodeStatsTest extends IntegTestCase {

    @Test
    public void testSysNodesMem() throws Exception {
        SQLResponse response = execute("select mem['free'], mem['used'], mem['free_percent'], mem['used_percent'] from sys.nodes limit 1");
        long free = (long) response.rows()[0][0];
        long used = (long) response.rows()[0][1];

        double free_percent = ((Number) response.rows()[0][2]).intValue() * 0.01;
        double used_percent = ((Number) response.rows()[0][3]).intValue() * 0.01;

        double calculated_free_percent = free / (double) (free + used);
        double calculated_used_percent = used / (double) (free + used);

        double max_delta = 0.02; // result should not differ from calculated result more than 2%
        double free_delta = Math.abs(calculated_free_percent - free_percent);
        double used_delta = Math.abs(calculated_used_percent - used_percent);
        assertThat(free_delta, is(lessThan(max_delta)));
        assertThat(used_delta, is(lessThan(max_delta)));

    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @UseJdbc(0) // because of json some values are transfered as integer instead of long
    public void testThreadPools() throws Exception {
        SQLResponse response = execute("select thread_pools from sys.nodes limit 1");

        List threadPools = (List) response.rows()[0][0];
        assertThat(threadPools.size(), greaterThanOrEqualTo(1));

        Map<String, Object> threadPool = null;
        for (Object t : threadPools) {
            Map<String, Object> map = (Map<String, Object>) t;
            if (map.get("name").equals("generic")) {
                threadPool = map;
                break;
            }
        }
        assertThat(threadPool.get("name"), is("generic"));
        assertThat((Integer) threadPool.get("active"), greaterThanOrEqualTo(0));
        assertThat((Long) threadPool.get("rejected"), greaterThanOrEqualTo(0L));
        assertThat((Integer) threadPool.get("largest"), greaterThanOrEqualTo(0));
        assertThat((Long) threadPool.get("completed"), greaterThanOrEqualTo(0L));
        assertThat((Integer) threadPool.get("threads"), greaterThanOrEqualTo(0));
        assertThat((Integer) threadPool.get("queue"), greaterThanOrEqualTo(0));
    }

    @Test
    public void testThreadPoolValue() throws Exception {
        SQLResponse response = execute("select thread_pools['name'], thread_pools['queue'] from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));

        List<Object> objects = (List<Object>) response.rows()[0][0];
        assertThat(objects, Matchers.hasItem("generic"));

        List queues = (List) response.rows()[0][1];
        assertThat(queues.size(), greaterThanOrEqualTo(1));
        assertThat((Integer) queues.get(0), greaterThanOrEqualTo(0));
    }

    @Test
    public void testNetwork() throws Exception {
        SQLResponse response = execute("select network from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));

        Map<String, Object> network = (Map<String, Object>) response.rows()[0][0];
        assertThat(network, hasKey("tcp"));
        Map<String, Object> tcp = (Map<String, Object>) network.get("tcp");
        assertNetworkTCP(tcp);


        response = execute("select network['tcp'] from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));
        tcp = (Map<String, Object>) response.rows()[0][0];
        assertNetworkTCP(tcp);
    }

    private void assertNetworkTCP(Map<String, Object> tcp) {
        assertThat(tcp.keySet().size(), is(2));
        assertThat(tcp.keySet(), hasItems("packets", "connections"));

        Map<String, Object> connections = (Map<String, Object>) tcp.get("connections");
        assertThat(connections.keySet().size(), is(5));
        assertThat(connections.keySet(), hasItems("initiated", "accepted", "curr_established", "dropped", "embryonic_dropped"));

        Map<String, Object> packets = (Map<String, Object>) tcp.get("packets");
        assertThat(packets.keySet().size(), is(5));
        assertThat(packets.keySet(), hasItems("sent", "received", "errors_received", "retransmitted", "rst_sent"));
    }

    @Test
    public void testNetworkTcpConnectionFields() throws Exception {
        SQLResponse response = execute("select " +
                                       "network['tcp']['connections']['initiated'], " +
                                       "network['tcp']['connections']['accepted'], " +
                                       "network['tcp']['connections']['curr_established']," +
                                       "network['tcp']['connections']['dropped']," +
                                       "network['tcp']['connections']['embryonic_dropped']" +
                                       " from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));
        for (int i = 0; i < response.cols().length; i++) {
            assertThat((Long) response.rows()[0][i], greaterThanOrEqualTo(-1L));
        }
    }

    @Test
    public void testNetworkTcpPacketsFields() throws Exception {
        SQLResponse response = execute("select " +
                                       "network['tcp']['packets']['sent'], " +
                                       "network['tcp']['packets']['received'], " +
                                       "network['tcp']['packets']['retransmitted'], " +
                                       "network['tcp']['packets']['errors_received'], " +
                                       "network['tcp']['packets']['rst_sent'] " +
                                       "from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));
        for (int i = 0; i < response.cols().length; i++) {
            assertThat((Long) response.rows()[0][i], greaterThanOrEqualTo(-1L));
        }
    }

    @Test
    @UseJdbc(0) // because of json some values are transfered as integer instead of long
    public void testSysNodesOs() throws Exception {
        SQLResponse response = execute("select os from sys.nodes limit 1");
        Map results = (Map) response.rows()[0][0];
        assertThat(response.rowCount(), is(1L));

        assertThat((Long) results.get("timestamp"), greaterThan(0L));
        assertThat((Long) results.get("uptime"), greaterThanOrEqualTo(-1L));

        assertThat((Short) ((Map) results.get("cpu")).get("system"), greaterThanOrEqualTo((short) -1));
        assertThat((Short) ((Map) results.get("cpu")).get("system"), lessThanOrEqualTo((short) 100));

        assertThat((Short) ((Map) results.get("cpu")).get("user"), greaterThanOrEqualTo((short) -1));
        assertThat((Short) ((Map) results.get("cpu")).get("user"), lessThanOrEqualTo((short) 100));

        assertThat((Short) ((Map) results.get("cpu")).get("used"), greaterThanOrEqualTo((short) -1));
        assertThat((Short) ((Map) results.get("cpu")).get("used"), lessThanOrEqualTo((short) 100));

    }

    @Test
    public void testSysNodesCgroup() throws Exception {
        if (Constants.LINUX && !"true".equals(System.getenv("SHIPPABLE"))) { // cgroups are only available on Linux
            SQLResponse response = execute("select" +
                                           " os['cgroup']['cpuacct']['control_group']," +
                                           " os['cgroup']['cpuacct']['usage_nanos']," +
                                           " os['cgroup']['cpu']['control_group']," +
                                           " os['cgroup']['cpu']['cfs_period_micros']," +
                                           " os['cgroup']['cpu']['cfs_quota_micros']," +
                                           " os['cgroup']['cpu']['num_elapsed_periods']," +
                                           " os['cgroup']['cpu']['num_times_throttled']," +
                                           " os['cgroup']['cpu']['time_throttled_nanos']" +
                                           " from sys.nodes limit 1");
            assertThat(response.rowCount(), is(1L));
            assertThat(response.rows()[0][0], notNullValue());
            assertThat((long) response.rows()[0][1], greaterThanOrEqualTo(0L));
            assertThat(response.rows()[0][2], notNullValue());
            assertThat((long) response.rows()[0][3], anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
            assertThat((long) response.rows()[0][4], anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
            assertThat((long) response.rows()[0][5], anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
            assertThat((long) response.rows()[0][6], anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
            assertThat((long) response.rows()[0][7], anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        } else {
            // for all other OS cgroup fields should return `null`
            response = execute("select os['cgroup']," +
                               " os['cgroup']['cpuacct']," +
                               " os['cgroup']['cpuacct']['control_group']," +
                               " os['cgroup']['cpuacct']['usage_nanos']," +
                               " os['cgroup']['cpu']," +
                               " os['cgroup']['cpu']['control_group']," +
                               " os['cgroup']['cpu']['cfs_period_micros']," +
                               " os['cgroup']['cpu']['cfs_quota_micros']," +
                               " os['cgroup']['cpu']['num_elapsed_periods']," +
                               " os['cgroup']['cpu']['num_times_throttled']," +
                               " os['cgroup']['cpu']['time_throttled_nanos']" +
                               " from sys.nodes limit 1");
            assertThat(response.rowCount(), is(1L));
            for (int i = 0; i <= 10; i++) {
                assertThat(response.rows()[0][1], Matchers.is(Map.of()));
            }
        }

    }

    @Test
    public void testSysNodsOsInfo() throws Exception {
        SQLResponse response = execute("select os_info from sys.nodes limit 1");
        Map results = (Map) response.rows()[0][0];
        assertThat(response.rowCount(), is(1L));

        assertThat((Integer) results.get("available_processors"), greaterThan(0));
        assertEquals(Constants.OS_NAME, results.get("name"));
        assertEquals(Constants.OS_ARCH, results.get("arch"));
        assertEquals(Constants.OS_VERSION, results.get("version"));

        Map<String, Object> jvmObj = new HashMap<>(4);
        java.lang.Runtime.Version version = Runtime.version();
        jvmObj.put("version", Lists2.joinOn(".", version.version(), num -> Integer.toString(num)));
        jvmObj.put("vm_name", Constants.JVM_NAME);
        jvmObj.put("vm_vendor", Constants.JVM_VENDOR);
        jvmObj.put("vm_version", version.toString());
        assertEquals(jvmObj, results.get("jvm"));
    }

    @Test
    public void testSysNodesProcess() throws Exception {
        SQLResponse response = execute("select process['open_file_descriptors'], " +
                                       "process['max_open_file_descriptors'] " +
                                       "from sys.nodes limit 1");
        for (int i = 0; i < response.cols().length; i++) {
            assertThat((Long) response.rows()[0][i], greaterThanOrEqualTo(-1L));
        }
    }

    @Test
    @UseJdbc(0) // because of json some values are transfered as integer instead of long
    public void testFs() throws Exception {
        SQLResponse response = execute("select fs from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], instanceOf(Map.class));
        Map<String, Object> fs = (Map<String, Object>) response.rows()[0][0];
        assertThat(fs.keySet().size(), is(3));
        assertThat(fs.keySet(), hasItems("total", "disks", "data"));

        Map<String, Object> total = (Map<String, Object>) fs.get("total");
        assertThat(total.keySet(), hasItems("size", "used", "available", "reads", "writes",
            "bytes_written", "bytes_read"));
        for (Object val : total.values()) {
            assertThat((Long) val, greaterThanOrEqualTo(-1L));
        }

        List disks = (List) fs.get("disks");
        if (disks.size() > 0) {
            // on travis there are no accessible disks
            assertThat(disks.size(), greaterThanOrEqualTo(1));
            Map<String, Object> someDisk = (Map<String, Object>) disks.get(0);
            assertThat(someDisk.keySet().size(), is(4));
            assertThat(someDisk.keySet(), hasItems("dev", "size", "used", "available"));
            for (Map.Entry<String, Object> entry : someDisk.entrySet()) {
                if (!entry.getKey().equals("dev")) {
                    assertThat((Long) entry.getValue(), greaterThanOrEqualTo(-1L));
                }
            }
        }

        List data = (List) fs.get("data");
        if (data.size() > 0) {
            // without sigar, no data definition returned
            int numDataPaths = cluster().getInstance(NodeEnvironment.class).nodeDataPaths().length;
            assertThat(data.size(), is(numDataPaths));
            Map<String, Object> someData = (Map<String, Object>) data.get(0);
            assertThat(someData.keySet().size(), is(2));
            assertThat(someData.keySet(), hasItems("dev", "path"));
        }
    }

    @Test
    public void testFsNoRootFS() throws Exception {
        SQLResponse response = execute("select fs['data']['dev'], fs['disks'] from sys.nodes");
        assertThat(response.rowCount(), is(2L));
        for (Object[] row : response.rows()) {
            // data device name
            for (Object diskDevName : (List) row[0]) {
                assertThat(diskDevName, is(not("rootfs")));
            }
            List disks = (List) row[1];
            // disks device name
            for (Object disk : disks) {
                String diskDevName = (String) ((Map<String, Object>) disk).get("dev");
                assertThat(diskDevName, is(notNullValue()));
                assertThat(diskDevName, is(not("rootfs")));
            }
        }
    }

    @Test
    public void testSysNodesObjectArrayStringChildColumn() throws Exception {
        SQLResponse response = execute("select fs['data']['path'] from sys.nodes");
        assertThat(response.rowCount(), Matchers.is(2L));
        for (Object path : (List) response.rows()[0][0]) {
            assertThat(path, instanceOf(String.class));
        }
    }

    @Test
    public void testVersion() throws Exception {
        SQLResponse response = execute("select version, version['number'], " +
                                       "version['build_hash'], version['build_snapshot'] " +
                                       "from sys.nodes limit 1");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], instanceOf(Map.class));
        assertThat((Map<String, Object>) response.rows()[0][0], allOf(hasKey("number"), hasKey("build_hash"), hasKey("build_snapshot")));
        assertThat((String) response.rows()[0][1], is(Version.CURRENT.externalNumber()));
        assertThat(response.rows()[0][2], instanceOf(String.class));
        assertThat((Boolean) response.rows()[0][3], is(Version.CURRENT.isSnapshot()));
    }

    @Test
    public void testRegexpMatchOnNode() throws Exception {
        SQLResponse response = execute("select name from sys.nodes where name ~ 'node_s[0-1]{1,2}' order by name");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("node_s0"));
        assertThat((String) response.rows()[1][0], is("node_s1"));
    }
}
