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

import static io.crate.testing.Asserts.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.IntegTestCase;
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
        assertThat(free_delta).isLessThan(max_delta);
        assertThat(used_delta).isLessThan(max_delta);

    }

    @SuppressWarnings("unchecked")
    @Test
    @UseJdbc(0) // because of json some values are transfered as integer instead of long
    public void testThreadPools() throws Exception {
        SQLResponse response = execute("select thread_pools from sys.nodes limit 1");

        List<?> threadPools = (List<?>) response.rows()[0][0];
        assertThat(threadPools).hasSizeGreaterThanOrEqualTo(1);

        Map<String, Object> threadPool = null;
        for (Object t : threadPools) {
            Map<String, Object> map = (Map<String, Object>) t;
            if (map.get("name").equals("generic")) {
                threadPool = map;
                break;
            }
        }
        assertThat(threadPool.get("name")).isEqualTo("generic");
        assertThat((Integer) threadPool.get("active")).isGreaterThanOrEqualTo(0);
        assertThat((Long) threadPool.get("rejected")).isGreaterThanOrEqualTo(0L);
        assertThat((Integer) threadPool.get("largest")).isGreaterThanOrEqualTo(0);
        assertThat((Long) threadPool.get("completed")).isGreaterThanOrEqualTo(0L);
        assertThat((Integer) threadPool.get("threads")).isGreaterThanOrEqualTo(0);
        assertThat((Integer) threadPool.get("queue")).isGreaterThanOrEqualTo(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testThreadPoolValue() throws Exception {
        SQLResponse response = execute("select thread_pools['name'], thread_pools['queue'] from sys.nodes limit 1");
        assertThat(response).hasRowCount(1L);

        List<Object> objects = (List<Object>) response.rows()[0][0];
        assertThat(objects).contains("generic");

        List<?> queues = (List<?>) response.rows()[0][1];
        assertThat(queues.size()).isGreaterThanOrEqualTo(1);
        assertThat((Integer) queues.get(0)).isGreaterThanOrEqualTo(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNetwork() throws Exception {
        SQLResponse response = execute("select network from sys.nodes limit 1");
        assertThat(response).hasRowCount(1L);

        Map<String, Object> network = (Map<String, Object>) response.rows()[0][0];
        assertThat(network).containsKey("tcp");
        Map<String, Object> tcp = (Map<String, Object>) network.get("tcp");
        assertNetworkTCP(tcp);


        response = execute("select network['tcp'] from sys.nodes limit 1");
        assertThat(response).hasRowCount(1L);
        tcp = (Map<String, Object>) response.rows()[0][0];
        assertNetworkTCP(tcp);
    }

    @SuppressWarnings("unchecked")
    private void assertNetworkTCP(Map<String, Object> tcp) {
        assertThat(tcp).containsOnlyKeys("packets", "connections");

        Map<String, Object> connections = (Map<String, Object>) tcp.get("connections");
        assertThat(connections).containsOnlyKeys(
            "initiated", "accepted", "curr_established", "dropped", "embryonic_dropped");

        Map<String, Object> packets = (Map<String, Object>) tcp.get("packets");
        assertThat(packets).containsOnlyKeys(
            "sent", "received", "errors_received", "retransmitted", "rst_sent");
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
        assertThat(response).hasRowCount(1L);
        for (int i = 0; i < response.cols().length; i++) {
            assertThat((Long) response.rows()[0][i]).isGreaterThanOrEqualTo(-1L);
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
        assertThat(response).hasRowCount(1L);
        for (int i = 0; i < response.cols().length; i++) {
            assertThat((Long) response.rows()[0][i]).isGreaterThanOrEqualTo(-1L);
        }
    }

    @Test
    @UseJdbc(0) // because of json some values are transfered as integer instead of long
    public void testSysNodesOs() throws Exception {
        SQLResponse response = execute("select os from sys.nodes limit 1");
        Map<?, ?> results = (Map<?, ?>) response.rows()[0][0];
        assertThat(response).hasRowCount(1L);

        assertThat((Long) results.get("timestamp")).isGreaterThan(0L);
        assertThat((Long) results.get("uptime")).isGreaterThanOrEqualTo(-1L);

        assertThat((Short) ((Map<?, ?>) results.get("cpu")).get("system")).isGreaterThanOrEqualTo((short) -1);
        assertThat((Short) ((Map<?, ?>) results.get("cpu")).get("system")).isLessThanOrEqualTo((short) 100);

        assertThat((Short) ((Map<?, ?>) results.get("cpu")).get("user")).isGreaterThanOrEqualTo((short) -1);
        assertThat((Short) ((Map<?, ?>) results.get("cpu")).get("user")).isLessThanOrEqualTo((short) 100);

        assertThat((Short) ((Map<?, ?>) results.get("cpu")).get("used")).isGreaterThanOrEqualTo((short) -1);
        assertThat((Short) ((Map<?, ?>) results.get("cpu")).get("used")).isLessThanOrEqualTo((short) 100);

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
            assertThat(response).hasRowCount(1L);
            assertThat(response.rows()[0][0]).isNotNull();
            assertThat((long) response.rows()[0][1]).isGreaterThanOrEqualTo(0L);
            assertThat(response.rows()[0][2]).isNotNull();
            assertThat((long) response.rows()[0][3])
                .satisfiesAnyOf(x -> assertThat(x).isEqualTo(-1), x -> assertThat(x).isGreaterThanOrEqualTo(0));
            assertThat((long) response.rows()[0][4])
                .satisfiesAnyOf(x -> assertThat(x).isEqualTo(-1), x -> assertThat(x).isGreaterThanOrEqualTo(0));
            assertThat((long) response.rows()[0][5])
                .satisfiesAnyOf(x -> assertThat(x).isEqualTo(-1), x -> assertThat(x).isGreaterThanOrEqualTo(0));
            assertThat((long) response.rows()[0][6])
                .satisfiesAnyOf(x -> assertThat(x).isEqualTo(-1), x -> assertThat(x).isGreaterThanOrEqualTo(0));
            assertThat((long) response.rows()[0][7])
                .satisfiesAnyOf(x -> assertThat(x).isEqualTo(-1), x -> assertThat(x).isGreaterThanOrEqualTo(0));
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
            assertThat(response).hasRowCount(1L);
            for (int i = 0; i <= 10; i++) {
                assertThat(response.rows()[0][1]).isEqualTo(Map.of());
            }
        }

    }

    @Test
    public void testSysNodsOsInfo() throws Exception {
        SQLResponse response = execute("select os_info from sys.nodes limit 1");
        Map<?, ?> results = (Map<?, ?>) response.rows()[0][0];
        assertThat(response).hasRowCount(1L);

        assertThat((Integer) results.get("available_processors")).isGreaterThan(0);
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
            assertThat((Long) response.rows()[0][i]).isGreaterThanOrEqualTo(-1L);
        }
    }

    @Test
    @UseJdbc(0) // because of json some values are transfered as integer instead of long
    @SuppressWarnings("unchecked")
    public void testFs() throws Exception {
        SQLResponse response = execute("select fs from sys.nodes limit 1");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isInstanceOf(Map.class);
        Map<String, Object> fs = (Map<String, Object>) response.rows()[0][0];
        assertThat(fs).containsOnlyKeys("total", "disks", "data");

        Map<String, Object> total = (Map<String, Object>) fs.get("total");
        assertThat(total).containsKeys(
            "size", "used", "available", "reads", "writes", "bytes_written", "bytes_read");
        for (Object val : total.values()) {
            assertThat((Long) val).isGreaterThanOrEqualTo(-1L);
        }

        List<?> disks = (List<?>) fs.get("disks");
        if (disks.size() > 0) {
            // on travis there are no accessible disks
            assertThat(disks.size()).isGreaterThanOrEqualTo(1);
            Map<String, Object> someDisk = (Map<String, Object>) disks.get(0);
            assertThat(someDisk).containsOnlyKeys("dev", "size", "used", "available");
            for (Map.Entry<String, Object> entry : someDisk.entrySet()) {
                if (!entry.getKey().equals("dev")) {
                    assertThat((Long) entry.getValue()).isGreaterThanOrEqualTo(-1L);
                }
            }
        }

        List<?> data = (List<?>) fs.get("data");
        if (data.size() > 0) {
            // without sigar, no data definition returned
            NodeEnvironment instance = cluster().getInstance(NodeEnvironment.class);
            int numDataPaths = instance.nodeDataPaths().length;
            assertThat(data).hasSize(numDataPaths);
            Map<String, Object> someData = (Map<String, Object>) data.get(0);
            assertThat(someData).containsOnlyKeys("dev", "path");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFsNoRootFS() throws Exception {
        SQLResponse response = execute("select fs['data']['dev'], fs['disks'] from sys.nodes");
        assertThat(response).hasRowCount(2L);
        for (Object[] row : response.rows()) {
            // data device name
            for (Object diskDevName : (List<?>) row[0]) {
                assertThat(diskDevName).isNotEqualTo("rootfs");
            }
            List<?> disks = (List<?>) row[1];
            // disks device name
            for (Object disk : disks) {
                String diskDevName = (String) ((Map<String, Object>) disk).get("dev");
                assertThat(diskDevName).isNotNull();
                assertThat(diskDevName).isNotEqualTo("rootfs");
            }
        }
    }

    @Test
    public void testSysNodesObjectArrayStringChildColumn() throws Exception {
        SQLResponse response = execute("select fs['data']['path'] from sys.nodes");
        assertThat(response).hasRowCount(2L);
        for (Object path : (List<?>) response.rows()[0][0]) {
            assertThat(path).isExactlyInstanceOf(String.class);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testVersion() throws Exception {
        SQLResponse response = execute("select version, version['number'], " +
                                       "version['build_hash'], version['build_snapshot'] " +
                                       "from sys.nodes limit 1");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isInstanceOf(Map.class);
        assertThat((Map<String, Object>) response.rows()[0][0]).containsOnlyKeys(
            "number",
            "build_hash",
            "build_snapshot",
            "minimum_index_compatibility_version",
            "minimum_wire_compatibility_version"
        );
        assertThat((String) response.rows()[0][1]).isEqualTo(Version.CURRENT.externalNumber());
        assertThat(response.rows()[0][2]).isExactlyInstanceOf(String.class);
        assertThat((Boolean) response.rows()[0][3]).isEqualTo(Version.CURRENT.isSnapshot());
    }

    @Test
    public void testRegexpMatchOnNode() throws Exception {
        SQLResponse response = execute("select name from sys.nodes where name ~ 'node_s[0-1]{1,2}' order by name");
        assertThat(response).hasRowCount(2L);
        assertThat((String) response.rows()[0][0]).isEqualTo("node_s0");
        assertThat((String) response.rows()[1][0]).isEqualTo("node_s1");
    }
}
