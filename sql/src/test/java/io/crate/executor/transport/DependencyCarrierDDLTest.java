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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.testing.TestingRowConsumer;
import io.crate.es.action.admin.indices.alias.Alias;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.metadata.MetaData;
import io.crate.es.common.collect.MapBuilder;
import io.crate.es.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.isRow;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DependencyCarrierDDLTest extends SQLTransportIntegrationTest {

    private DependencyCarrier executor;

    private final static Map<String, Object> TEST_PARTITIONED_MAPPING = ImmutableMap.<String, Object>of(
        "_meta", ImmutableMap.of(
            "partitioned_by", ImmutableList.of(Arrays.asList("name", "string"))
        ),
        "properties", ImmutableMap.of(
            "id", ImmutableMap.builder()
                .put("type", "integer").build(),
            "names", ImmutableMap.builder()
                .put("type", "keyword").build()
        ));
    private final static Settings TEST_SETTINGS = Settings.builder()
        .put("number_of_replicas", 0)
        .put("number_of_shards", 2).build();

    @Before
    public void transportSetup() {
        executor = internalCluster().getInstance(DependencyCarrier.class);
    }

    @After
    public void resetSettings() throws Exception {
        client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(MapBuilder.newMapBuilder().put("stats.enabled", null).map())
            .setTransientSettings(MapBuilder.newMapBuilder().put("stats.enabled", null).put("bulk.request_timeout", null).map())
            .execute().actionGet();
    }

    @Test
    public void testCreateTableWithOrphanedPartitions() throws Exception {
        String partitionName = IndexParts.toIndexName(
            sqlExecutor.getCurrentSchema(),
            "test",
            PartitionName.encodeIdent(singletonList("foo"))
        );
        client().admin().indices().prepareCreate(partitionName)
            .addMapping(Constants.DEFAULT_MAPPING_TYPE, TEST_PARTITIONED_MAPPING)
            .setSettings(TEST_SETTINGS)
            .execute().actionGet();
        ensureGreen();

        execute("create table test (id integer, name string, names string) partitioned by (id)");
        ensureYellow();
        execute("select * from information_schema.tables where table_name = 'test'");
        assertThat(response.rowCount(), is(1L));

        execute("select count(*) from information_schema.columns where table_name = 'test'");
        assertThat((Long) response.rows()[0][0], is(3L));

        // check that orphaned partition has been deleted
        assertThat(internalCluster().clusterService().state().metaData().hasIndex(partitionName), is(false));
    }

    @Test
    public void testCreateTableWithOrphanedAlias() throws Exception {
        String partitionName = IndexParts.toIndexName(
            sqlExecutor.getCurrentSchema(), "test", PartitionName.encodeIdent(singletonList("foo")));
        client().admin().indices().prepareCreate(partitionName)
            .addMapping(Constants.DEFAULT_MAPPING_TYPE, TEST_PARTITIONED_MAPPING)
            .setSettings(TEST_SETTINGS)
            .addAlias(new Alias("test"))
            .execute().actionGet();
        ensureGreen();

        execute("create table test (id integer, name string, names string) " +
                "clustered into 2 shards " +
                "partitioned by (id) with (number_of_replicas=0)");
        assertThat(response.rowCount(), is(1L));
        ensureGreen();

        execute("select * from information_schema.tables where table_name = 'test'");
        assertThat(response.rowCount(), is(1L));

        execute("select count(*) from information_schema.columns where table_name = 'test'");
        assertThat((Long) response.rows()[0][0], is(3L));

        ClusterState state = internalCluster().clusterService().state();

        // check that orphaned alias has been deleted
        assertThat(state.metaData().hasAlias("test"), is(false));
        // check that orphaned partition has been deleted

        assertThat(state.metaData().hasIndex(partitionName), is(false));
    }


    /**
     * this case should not happen as closed indices aren't listed as TableInfo
     * but if it does maybe because of stale cluster state - validate behaviour here
     * <p>
     * cannot prevent this task from deleting closed indices.
     */
    @Test
    public void testDeletePartitionTaskClosed() throws Exception {
        execute("create table t (id integer primary key, name string) partitioned by (id)");
        ensureYellow();

        execute("insert into t (id, name) values (1, 'Ford')");
        assertThat(response.rowCount(), is(1L));
        ensureYellow();

        PlanForNode plan = plan("delete from t where id = ?");

        execute("alter table t partition (id = 1) close");

        Bucket bucket = executePlan(plan.plan, plan.plannerContext, new Row1(1));
        assertThat(bucket, contains(isRow(-1L)));

        execute("select * from information_schema.table_partitions where table_name = 't'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testClusterUpdateSettingsTask() throws Exception {
        final String persistentSetting = "stats.enabled";
        final String transientSetting = "bulk.request_timeout";

        // Update persistent only
        Map<String, List<Expression>> persistentSettings = new HashMap<String, List<Expression>>(){{
            put(persistentSetting, ImmutableList.<Expression>of(Literal.fromObject(false)));
        }};

        UpdateSettingsPlan node = new UpdateSettingsPlan(persistentSettings);
        PlannerContext plannerContext = mock(PlannerContext.class);
        Bucket objects = executePlan(node, plannerContext);

        assertThat(objects, contains(isRow(1L)));
        assertEquals("false", client().admin().cluster().prepareState().execute().actionGet().getState().metaData()
            .persistentSettings().get(persistentSetting)
        );

        // Update transient only
        Map<String, List<Expression>> transientSettings = new HashMap<String, List<Expression>>(){{
            put(transientSetting, ImmutableList.<Expression>of(Literal.fromObject("123s")));
        }};

        node = new UpdateSettingsPlan(ImmutableMap.<String, List<Expression>>of(), transientSettings);
        objects = executePlan(node, plannerContext);

        assertThat(objects, contains(isRow(1L)));
        assertEquals("123s", client().admin().cluster().prepareState().execute().actionGet().getState().metaData()
            .transientSettings().get(transientSetting)
        );

        // Update persistent & transient
        persistentSettings = new HashMap<String, List<Expression>>(){{
            put(persistentSetting, ImmutableList.<Expression>of(Literal.fromObject(false)));
        }};

        transientSettings = new HashMap<String, List<Expression>>(){{
            put(transientSetting, ImmutableList.<Expression>of(Literal.fromObject("243s")));
        }};

        node = new UpdateSettingsPlan(persistentSettings, transientSettings);
        objects = executePlan(node, plannerContext);

        MetaData md = client().admin().cluster().prepareState().execute().actionGet().getState().metaData();
        assertThat(objects, contains(isRow(1L)));
        assertEquals("false", md.persistentSettings().get(persistentSetting));
        assertEquals("243s", md.transientSettings().get(transientSetting));
    }

    private Bucket executePlan(Plan plan, PlannerContext plannerContext, Row params) throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        plan.execute(
            executor,
            plannerContext,
            consumer,
            params,
            SubQueryResults.EMPTY
        );
        return consumer.getBucket();
    }

    private Bucket executePlan(Plan plan, PlannerContext plannerContext) throws Exception {
        return executePlan(plan, plannerContext, Row.EMPTY);
    }
}
