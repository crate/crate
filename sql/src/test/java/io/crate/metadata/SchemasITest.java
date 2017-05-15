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
import io.crate.analyze.WhereClause;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class SchemasITest extends SQLTransportIntegrationTest {

    private Schemas schemas;

    @Before
    public void setUpService() {
        schemas = internalCluster().getInstance(Schemas.class);
    }

    @Test
    public void testDocTable() throws Exception {
        execute("create table t1 (" +
                "id int primary key, " +
                "name string, " +
                "details object(dynamic) as (size byte, created timestamp)" +
                ") clustered into 10 shards with (number_of_replicas=1)");
        ensureYellow();

        DocTableInfo ti = (DocTableInfo) schemas.getTableInfo(new TableIdent(null, "t1"), null);
        assertThat(ti.ident().name(), is("t1"));

        assertThat(ti.columns().size(), is(3));
        assertThat(ti.primaryKey().size(), is(1));
        assertThat(ti.primaryKey().get(0), is(new ColumnIdent("id")));
        assertThat(ti.clusteredBy(), is(new ColumnIdent("id")));

        Routing routing = ti.getRouting(WhereClause.MATCH_ALL, null);

        Set<String> nodes = routing.nodes();

        assertThat(nodes.size(), isOneOf(1, 2)); // for the rare case
        // where all shards are on 1 node
        int numShards = 0;
        for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, List<Integer>> indexEntry : nodeEntry.getValue().entrySet()) {
                assertThat(indexEntry.getKey(), is("t1"));
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards, is(10));
    }

    @Test
    public void testTableAlias() throws Exception {
        execute("create table terminator (model string, good boolean, actor object)");
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add()
            .alias("entsafter")
            .index("terminator"));
        client().admin().indices().aliases(request).actionGet();
        ensureYellow();

        DocTableInfo terminatorTable = (DocTableInfo) schemas.getTableInfo(new TableIdent(null, "terminator"), null);
        DocTableInfo entsafterTable = (DocTableInfo) schemas.getTableInfo(new TableIdent(null, "entsafter"), null);

        assertNotNull(terminatorTable);
        assertFalse(terminatorTable.isAlias());

        assertNotNull(entsafterTable);
        assertTrue(entsafterTable.isAlias());
    }

    @Test
    public void testAliasPartitions() throws Exception {
        execute("create table terminator (model string, good boolean, actor object)");
        execute("create table transformer (model string, good boolean, actor object)");
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(
            IndicesAliasesRequest.AliasActions.add().alias("entsafter").index("terminator"));
        request.addAliasAction(
            IndicesAliasesRequest.AliasActions.add().alias("entsafter").index("transformer"));
        client().admin().indices().aliases(request).actionGet();
        ensureYellow();

        DocTableInfo entsafterTable = (DocTableInfo) schemas.getTableInfo(new TableIdent(null, "entsafter"), null);

        assertNotNull(entsafterTable);
        assertThat(entsafterTable.concreteIndices().length, is(2));
        assertThat(Arrays.asList(entsafterTable.concreteIndices()), containsInAnyOrder("terminator", "transformer"));
    }


    @Test
    public void testNodesTable() throws Exception {
        TableInfo ti = schemas.getTableInfo(new TableIdent("sys", "nodes"), null);
        Routing routing = ti.getRouting(null, null);
        assertTrue(routing.hasLocations());
        assertEquals(1, routing.nodes().size());
        for (Map<String, List<Integer>> indices : routing.locations().values()) {
            assertEquals(1, indices.size());
        }
    }

    @Test
    public void testShardsTable() throws Exception {
        execute("create table t2 (id int primary key) clustered into 4 shards with(number_of_replicas=0)");
        execute("create table t3 (id int primary key) clustered into 8 shards with(number_of_replicas=0)");
        ensureYellow();

        TableInfo ti = schemas.getTableInfo(new TableIdent("sys", "shards"), null);
        Routing routing = ti.getRouting(null, null);

        Set<String> tables = new HashSet<>();
        Set<String> expectedTables = Sets.newHashSet("t2", "t3");
        int numShards = 0;
        for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : routing.locations().entrySet()) {
            for (Map.Entry<String, List<Integer>> indexEntry : nodeEntry.getValue().entrySet()) {
                tables.add(indexEntry.getKey());
                numShards += indexEntry.getValue().size();
            }
        }
        assertThat(numShards, is(12));
        assertThat(tables, is(expectedTables));
    }

    @Test
    public void testClusterTable() throws Exception {
        TableInfo ti = schemas.getTableInfo(new TableIdent("sys", "cluster"), null);
        assertThat(ti.getRouting(null, null).locations().size(), is(1));
    }
}
