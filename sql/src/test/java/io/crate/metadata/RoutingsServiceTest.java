/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import com.google.common.collect.Sets;
import io.crate.metadata.table.TableInfo;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.ClusterService;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class RoutingsServiceTest extends SQLTransportIntegrationTest {

    private RoutingsService routingsService;
    private ClusterService clusterService;
    private ReferenceInfos referenceInfos;

    @Before
    public void setUpService() {
        clusterService = cluster().getInstance(ClusterService.class);
        routingsService = new RoutingsService(clusterService);
        referenceInfos = cluster().getInstance(ReferenceInfos.class);
    }

    @Test
    public void testDocRouting() throws Exception {
        execute("create table t1 (id int primary key) clustered into 10 shards replicas 1");
        ensureGreen();

        Routing routing = routingsService.getRouting(new TableIdent(null, "t1"));
        Set<String> nodes = routing.nodes();

        assertThat(nodes.size(), is(2));
        int numShards = 0;
        for (Map.Entry<String, Map<String, Set<Integer>>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, Set<Integer>> indexEntry : nodeEntry.getValue().entrySet()) {
                assertThat(indexEntry.getKey(), is("t1"));
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards, is(10));
    }

    @Test
    public void testNodesRouting() throws Exception {
        TableInfo ti = referenceInfos.getTableInfo(new TableIdent("sys", "nodes"));
        Routing routing = ti.getRouting(null);
        assertTrue(routing.hasLocations());
        assertEquals(2, routing.nodes().size());
        for (Map<String, Set<Integer>> indices : routing.locations().values()) {
            assertEquals(0, indices.size());
        }
    }

    @Test
    public void testShardsRouting() throws Exception {
        execute("create table t2 (id int primary key) clustered into 4 shards replicas 0");
        execute("create table t3 (id int primary key) clustered into 8 shards replicas 0");
        ensureGreen();

        TableInfo ti = referenceInfos.getTableInfo(new TableIdent("sys", "shards"));
        Routing routing = ti.getRouting(null);

        Set<String> tables = new HashSet<>();
        Set<String> expectedTables = Sets.newHashSet("t2", "t3");
        int numShards = 0;
        for (Map.Entry<String, Map<String, Set<Integer>>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, Set<Integer>> indexEntry : nodeEntry.getValue().entrySet()) {
                tables.add(indexEntry.getKey());
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards, is(12));
        assertThat(tables, is(expectedTables));
    }

    @Test
    public void testClusterRouting() throws Exception {
        TableInfo ti = referenceInfos.getTableInfo(new TableIdent("sys", "cluster"));
        assertNull(ti.getRouting(null));
    }
}
