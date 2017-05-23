/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import io.crate.analyze.TableDefinitions;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.PartitionName;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.PrimaryKeyLookupPhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.crate.testing.TestingHelpers.isDocKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests involving selects with WHERE clauses using primary keys.
 * In addition to {@link SelectPlannerTest}.
 */
public class SelectPlannerPKTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {

        List<String> indices = new ArrayList<>();
        indices.add(TableDefinitions.USER_TABLE_IDENT.indexName());
        indices.addAll(TableDefinitions.PARTED_PKS_TI.partitions().stream().map(PartitionName::asIndexName).collect(Collectors.toList()));
        indices.add(TableDefinitions.TEST_CLUSTER_BY_STRING_TABLE_INFO.ident().indexName());
        indices.add(".partitioned.parted_pks.04130");

        prepareRoutingForIndices(indices);

        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_CLUSTER_BY_STRING_TABLE_INFO)
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .addDocTable(TableDefinitions.TEST_PARTITIONED_TABLE_INFO)
            .addDocTable(TableDefinitions.IGNORED_NESTED_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_MULTIPLE_PARTITIONED_TABLE_INFO)
            .addDocTable(T3.T1_INFO)
            .build();
    }

    @Test
    public void testWherePKAndMatchDoesNotResultInESGet() throws Exception {
        Plan plan = e.plan("select * from users where id in (1, 2, 3) and match(text, 'Hello')");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        assertThat(((QueryThenFetch)plan).subPlan(), not(instanceOf(PrimaryKeyLookupPhase.class)));
    }

    @Test
    public void testGetPlan() throws Exception {
        Merge plan = e.plan("select name from users where id = 1");
        assertThat(plan.subPlan(), instanceOf(Collect.class));
        Collect collect = (Collect) plan.subPlan();
        assertThat(collect.numOutputs(), is(1));
        assertThat(collect.collectPhase(), instanceOf(PrimaryKeyLookupPhase.class));
        PrimaryKeyLookupPhase pkLookupPhase = (PrimaryKeyLookupPhase) collect.collectPhase();
        assertThat(pkLookupPhase.docKeys().getOnlyKey(), isDocKey(1L));
    }

    @Test
    public void testGetWithVersion() throws Exception {
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        Merge plan = e.plan("select name from users where id = 1 and _version = 1");
        assertThat(plan.subPlan(), instanceOf(Collect.class));
        Collect collect = (Collect) plan.subPlan();
        assertThat(collect.numOutputs(), is(1));
        assertThat(collect.collectPhase(), instanceOf(PrimaryKeyLookupPhase.class));
    }

    @Test
    public void testGetPlanStringLiteral() throws Exception {
        Merge plan = e.plan("select name from bystring where name = 'one'");
        assertThat(plan.subPlan(), instanceOf(Collect.class));
        Collect collect = (Collect) plan.subPlan();
        assertThat(collect.numOutputs(), is(1));
        assertThat(collect.collectPhase(), instanceOf(PrimaryKeyLookupPhase.class));
        PrimaryKeyLookupPhase pkLookupPhase = (PrimaryKeyLookupPhase) collect.collectPhase();
        assertThat(pkLookupPhase.docKeys().getOnlyKey(), isDocKey("one"));
    }

    @Test
    public void testGetPlanPartitioned() throws Exception {
        Merge plan = e.plan("select name, date from parted_pks where id = 1 and date = 0");
        assertThat(plan.subPlan(), instanceOf(Collect.class));
        Collect collect = (Collect) plan.subPlan();
        assertThat(collect.numOutputs(), is(2));
        assertThat(collect.collectPhase(), instanceOf(PrimaryKeyLookupPhase.class));
        PrimaryKeyLookupPhase pkLookupPhase = (PrimaryKeyLookupPhase) collect.collectPhase();

        assertThat(pkLookupPhase.docKeys().getOnlyKey(), isDocKey(1, 0L));

        //is(new PartitionName("parted", Arrays.asList(new BytesRef("0"))).asIndexName()));
        assertEquals(DataTypes.STRING, collect.streamOutputs().get(0));
        assertEquals(DataTypes.TIMESTAMP, collect.streamOutputs().get(1));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        Merge plan = e.plan("select name from users where id in (1, 2)");
        assertThat(plan.subPlan(), instanceOf(Collect.class));
        Collect collect = (Collect) plan.subPlan();
        assertThat(collect.numOutputs(), is(1));
        assertThat(collect.collectPhase(), instanceOf(PrimaryKeyLookupPhase.class));
        PrimaryKeyLookupPhase pkLookupPhase = (PrimaryKeyLookupPhase) collect.collectPhase();
        assertThat(pkLookupPhase.docKeys().size(), is(2));
        assertThat(pkLookupPhase.docKeys(), containsInAnyOrder(isDocKey(1L), isDocKey(2L)));
    }

}
