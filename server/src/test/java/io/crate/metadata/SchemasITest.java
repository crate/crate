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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.analyze.WhereClause;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.CheckConstraint;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SchemasITest extends IntegTestCase {

    private Schemas schemas;
    private RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());

    @Before
    public void setUpService() {
        schemas = cluster().getInstance(NodeContext.class).schemas();
    }

    @Test
    public void testDocTable() {
        execute("create table t1 (" +
                "id int primary key, " +
                "name string, " +
                "CONSTRAINT not_miguel CHECK (name != 'miguel'), " +
                "details object(dynamic) as (size byte, created timestamp with time zone)" +
                ") clustered into 10 shards with (number_of_replicas=1)");

        DocTableInfo ti = schemas.getTableInfo(new RelationName(sqlExecutor.getCurrentSchema(), "t1"));
        assertThat(ti.ident().name()).isEqualTo("t1");

        assertThat(ti.rootColumns()).hasSize(3);
        assertThat(ti.primaryKey()).hasSize(1);
        assertThat(ti.primaryKey().get(0)).isEqualTo(ColumnIdent.of("id"));
        assertThat(ti.clusteredBy()).isEqualTo(ColumnIdent.of("id"));
        List<CheckConstraint<Symbol>> checkConstraints = ti.checkConstraints();
        assertThat(checkConstraints.size()).isEqualTo(1);
        assertThat("not_miguel").isEqualTo(checkConstraints.get(0).name());
        assertThat(checkConstraints.get(0).expressionStr()).isEqualTo("\"name\" <> 'miguel'");

        ClusterService clusterService = clusterService();
        Routing routing = ti.getRouting(
            clusterService.state(),
            routingProvider,
            WhereClause.MATCH_ALL,
            RoutingProvider.ShardSelection.ANY,
            CoordinatorSessionSettings.systemDefaults()
        );

        Set<String> nodes = routing.nodes();
        String indexUUID = resolveIndex("t1").getUUID();

        assertThat(nodes.size()).isBetween(1, 2); // for the rare case
        // where all shards are on 1 node
        int numShards = 0;
        for (Map.Entry<String, Map<String, IntIndexedContainer>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, IntIndexedContainer> indexEntry : nodeEntry.getValue().entrySet()) {
                assertThat(indexEntry.getKey()).isEqualTo(indexUUID);
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards).isEqualTo(10);
    }

    @Test
    public void testNodesTable() throws Exception {
        TableInfo ti = schemas.getTableInfo(new RelationName("sys", "nodes"));
        ClusterService clusterService = clusterService();
        Routing routing = ti.getRouting(
            clusterService.state(), routingProvider, null, null, CoordinatorSessionSettings.systemDefaults());
        assertThat(routing.hasLocations()).isTrue();
        assertThat(routing.nodes().size()).isEqualTo(1);
        for (Map<String, ?> indices : routing.locations().values()) {
            assertThat(indices.size()).isEqualTo(1);
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
            clusterService.state(), routingProvider, null, null, CoordinatorSessionSettings.systemDefaults());

        Set<String> indexUUIDs = new HashSet<>();
        Set<String> expectedIndexUUIDs = Set.of(resolveIndex("t2").getUUID(), resolveIndex("t3").getUUID());
        int numShards = 0;
        for (Map.Entry<String, Map<String, IntIndexedContainer>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, IntIndexedContainer> indexEntry : nodeEntry.getValue().entrySet()) {
                indexUUIDs.add(indexEntry.getKey());
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards).isEqualTo(12);
        assertThat(indexUUIDs).isEqualTo(expectedIndexUUIDs);
    }

    @Test
    public void testClusterTable() throws Exception {
        TableInfo ti = schemas.getTableInfo(new RelationName("sys", "cluster"));
        ClusterService clusterService = clusterService();
        assertThat(ti.getRouting(
            clusterService.state(), routingProvider, null, null, CoordinatorSessionSettings.systemDefaults()
        ).locations()).hasSize(1);
    }
}
