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

import com.carrotsearch.hppc.IntIndexedContainer;
import com.google.common.collect.Sets;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.symbol.Symbol;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.CheckConstraint;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SchemasITest extends SQLTransportIntegrationTest {

    private Schemas schemas;
    private RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());

    @Before
    public void setUpService() {
        schemas = internalCluster().getInstance(Schemas.class);
    }

    @Test
    public void testDocTable() {
        execute("create table t1 (" +
                "id int primary key, " +
                "name string, " +
                "CONSTRAINT not_miguel CHECK (name != 'miguel'), " +
                "details object(dynamic) as (size byte, created timestamp with time zone)" +
                ") clustered into 10 shards with (number_of_replicas=1)");
        ensureYellow();

        DocTableInfo ti = schemas.getTableInfo(new RelationName(sqlExecutor.getCurrentSchema(), "t1"));
        assertThat(ti.ident().name(), is("t1"));

        assertThat(ti.columns().size(), is(3));
        assertThat(ti.primaryKey().size(), is(1));
        assertThat(ti.primaryKey().get(0), is(new ColumnIdent("id")));
        assertThat(ti.clusteredBy(), is(new ColumnIdent("id")));
        List<CheckConstraint<Symbol>> checkConstraints = ti.checkConstraints();
        assertEquals(1, checkConstraints.size());
        assertEquals(checkConstraints.get(0).name(), "not_miguel");
        assertThat(checkConstraints.get(0).expressionStr(), equalTo("\"name\" <> 'miguel'"));

        ClusterService clusterService = clusterService();
        Routing routing = ti.getRouting(
            clusterService.state(),
            routingProvider,
            WhereClause.MATCH_ALL,
            RoutingProvider.ShardSelection.ANY,
            SessionContext.systemSessionContext()
        );

        Set<String> nodes = routing.nodes();

        assertThat(nodes.size(), isOneOf(1, 2)); // for the rare case
        // where all shards are on 1 node
        int numShards = 0;
        for (Map.Entry<String, Map<String, IntIndexedContainer>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, IntIndexedContainer> indexEntry : nodeEntry.getValue().entrySet()) {
                assertThat(indexEntry.getKey(), is(getFqn("t1")));
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards, is(10));
    }

    @Test
    public void testNodesTable() throws Exception {
        TableInfo ti = schemas.getTableInfo(new RelationName("sys", "nodes"));
        ClusterService clusterService = clusterService();
        Routing routing = ti.getRouting(
            clusterService.state(), routingProvider, null, null, SessionContext.systemSessionContext());
        assertTrue(routing.hasLocations());
        assertEquals(1, routing.nodes().size());
        for (Map<String, ?> indices : routing.locations().values()) {
            assertEquals(1, indices.size());
        }
    }

    @Test
    public void testShardsTable() throws Exception {
        execute("create table t2 (id int primary key) clustered into 4 shards with(number_of_replicas=0)");
        execute("create table t3 (id int primary key) clustered into 8 shards with(number_of_replicas=0)");
        ensureYellow();

        TableInfo ti = schemas.getTableInfo(new RelationName("sys", "shards"));
        ClusterService clusterService = clusterService();
        Routing routing = ti.getRouting(
            clusterService.state(), routingProvider, null, null, SessionContext.systemSessionContext());

        Set<String> tables = new HashSet<>();
        Set<String> expectedTables = Sets.newHashSet(getFqn("t2"), getFqn("t3"));
        int numShards = 0;
        for (Map.Entry<String, Map<String, IntIndexedContainer>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, IntIndexedContainer> indexEntry : nodeEntry.getValue().entrySet()) {
                tables.add(indexEntry.getKey());
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards, is(12));
        assertThat(tables, is(expectedTables));
    }

    @Test
    public void testClusterTable() throws Exception {
        TableInfo ti = schemas.getTableInfo(new RelationName("sys", "cluster"));
        ClusterService clusterService = clusterService();
        assertThat(ti.getRouting(
            clusterService.state(), routingProvider, null, null, SessionContext.systemSessionContext()
        ).locations().size(), is(1));
    }
}
