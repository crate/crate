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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.planner.Plan;
import io.crate.planner.node.ddl.CreateTableNode;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import io.crate.planner.node.ddl.ESCreateTemplateNode;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import io.crate.test.integration.CrateIntegrationTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorDDLTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private TransportExecutor executor;

    private final static Map<String, Object> TEST_MAPPING = ImmutableMap.<String, Object>of(
            "properties", ImmutableMap.of(
                    "id", ImmutableMap.builder()
                            .put("type", "integer")
                            .put("store", false)
                            .put("index", "not_analyzed")
                            .put("doc_values", true).build(),
                    "name", ImmutableMap.builder()
                            .put("type", "string")
                            .put("store", false)
                            .put("index", "not_analyzed")
                            .put("doc_values", true).build(),
                    "names", ImmutableMap.builder()
                            .put("type", "string")
                            .put("store", false)
                            .put("index", "not_analyzed")
                            .put("doc_values", false).build()
            ));
    private final static Map<String, Object> TEST_PARTITIONED_MAPPING = ImmutableMap.<String, Object>of(
            "_meta", ImmutableMap.of(
                    "partitioned_by", ImmutableList.of(Arrays.asList("name", "string"))
            ),
            "properties", ImmutableMap.of(
                "id", ImmutableMap.builder()
                    .put("type", "integer")
                    .put("store", false)
                    .put("index", "not_analyzed")
                    .put("doc_values", true).build(),
                "names", ImmutableMap.builder()
                    .put("type", "string")
                    .put("store", false)
                    .put("index", "not_analyzed")
                    .put("doc_values", false).build()
            ));
    private final static Settings TEST_SETTINGS = ImmutableSettings.settingsBuilder()
            .put("number_of_replicas", 0)
            .put("number_of_shards", 2).build();

    @Before
    public void transportSetup() {
        executor = cluster().getInstance(TransportExecutor.class);
    }

    @Test
    public void testCreateTableTask() throws Exception {
        CreateTableNode createTableNode = CreateTableNode.createTableNode(
                "test",
                TEST_SETTINGS,
                TEST_MAPPING
        );
        Plan plan = new Plan();
        plan.add(createTableNode);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();

        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));

        execute("select * from information_schema.tables where table_name = 'test' and number_of_replicas = 0 and number_of_shards = 2");
        assertThat(response.rowCount(), is(1L));

        execute("select count(*) from information_schema.columns where table_name = 'test'");
        assertThat((Long)response.rows()[0][0], is(3L));
    }

    @Test
    public void testCreateTableWithOrphanedPartitions() throws Exception {
        String partitionName = new PartitionName("test", Arrays.asList(new BytesRef("foo"))).stringValue();
        client().admin().indices().prepareCreate(partitionName)
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, TEST_PARTITIONED_MAPPING)
                .setSettings(TEST_SETTINGS)
                .execute().actionGet();
        ensureGreen();
        CreateTableNode createTableNode = CreateTableNode.createTableNode(
                "test",
                TEST_SETTINGS,
                TEST_MAPPING
        );
        Plan plan = new Plan();
        plan.add(createTableNode);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();

        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));

        execute("select * from information_schema.tables where table_name = 'test' and number_of_replicas = 0 and number_of_shards = 2");
        assertThat(response.rowCount(), is(1L));

        execute("select count(*) from information_schema.columns where table_name = 'test'");
        assertThat((Long)response.rows()[0][0], is(3L));

        // check that orphaned partition has been deleted
        assertThat(client().admin().indices().exists(new IndicesExistsRequest(partitionName)).actionGet().isExists(), is(false));
    }

    @Test
    public void testCreateTableWithOrphanedAlias() throws Exception {
        String partitionName = new PartitionName("test", Arrays.asList(new BytesRef("foo"))).stringValue();
        client().admin().indices().prepareCreate(partitionName)
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, TEST_PARTITIONED_MAPPING)
                .setSettings(TEST_SETTINGS)
                .addAlias(new Alias("test"))
                .execute().actionGet();
        ensureGreen();
        CreateTableNode createTableNode = CreateTableNode.createTableNode(
                "test",
                TEST_SETTINGS,
                TEST_MAPPING
        );
        Plan plan = new Plan();
        plan.add(createTableNode);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();

        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));

        execute("select * from information_schema.tables where table_name = 'test' and number_of_replicas = 0 and number_of_shards = 2");
        assertThat(response.rowCount(), is(1L));

        execute("select count(*) from information_schema.columns where table_name = 'test'");
        assertThat((Long) response.rows()[0][0], is(3L));

        // check that orphaned partition has been deleted
        assertThat(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().aliases().containsKey("test"), is(false));
        // check that orphaned partition has been deleted
        assertThat(client().admin().indices().exists(new IndicesExistsRequest(partitionName)).actionGet().isExists(), is(false));
    }

    @Test
    public void testDeleteIndexTask() throws Exception {
        execute("create table t (id integer primary key, name string)");
        ensureGreen();

        execute("select * from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(1L));

        ESDeleteIndexNode deleteIndexNode = new ESDeleteIndexNode("t");
        Plan plan = new Plan();
        plan.add(deleteIndexNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();
        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));

        execute("select * from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(0L));
    }

    /**
     * this case should not happen as closed indices aren't listed as TableInfo
     * but if it does maby because of stale cluster state - validate behaviour here
     *
     * cannot prevent this task from deleting closed indices.
     */
    @Test
    public void testDeleteIndexTaskClosed() throws Exception {
        execute("create table t (id integer primary key, name string)");
        ensureGreen();
        assertTrue(client().admin().indices().prepareClose("t").execute().actionGet().isAcknowledged());

        ESDeleteIndexNode deleteIndexNode = new ESDeleteIndexNode("t");
        Plan plan = new Plan();
        plan.add(deleteIndexNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();
        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));

        execute("select * from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testClusterUpdateSettingsTask() throws Exception {
        final String persistentSetting = "persistent.level";
        final String transientSetting = "transient.uptime";
        // allow our settings to be updated (at all nodes)
        DynamicSettings dynamicSettings;
        for (int i = 0; i < cluster().size(); i++) {
            dynamicSettings = cluster().getInstance(Key.get(DynamicSettings.class, ClusterDynamicSettings.class), "node_" + i);
            dynamicSettings.addDynamicSetting(persistentSetting);
            dynamicSettings.addDynamicSetting(transientSetting);
        }

        // Update persistent only
        Settings persistentSettings = ImmutableSettings.builder()
                .put(persistentSetting, "panic")
                .build();

        ESClusterUpdateSettingsNode node = new ESClusterUpdateSettingsNode(persistentSettings);

        Plan plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();
        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));
        assertEquals("panic", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().persistentSettings().get(persistentSetting));

        // Update transient only
        Settings transientSettings = ImmutableSettings.builder()
                .put(transientSetting, "123")
                .build();

        node = new ESClusterUpdateSettingsNode(EMPTY_SETTINGS, transientSettings);

        plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(true);

        job = executor.newJob(plan);
        futures = executor.execute(job);
        listenableFuture = Futures.allAsList(futures);
        taskResult = listenableFuture.get().get(0);
        objects = listenableFuture.get().get(0).rows();
        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));
        assertEquals("123", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().transientSettings().get(transientSetting));

        // Update persistent & transient
        persistentSettings = ImmutableSettings.builder()
                .put(persistentSetting, "normal")
                .build();
        transientSettings = ImmutableSettings.builder()
                .put(transientSetting, "243")
                .build();

        node = new ESClusterUpdateSettingsNode(persistentSettings, transientSettings);

        plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(true);

        job = executor.newJob(plan);
        futures = executor.execute(job);
        listenableFuture = Futures.allAsList(futures);
        taskResult = listenableFuture.get().get(0);
        objects = listenableFuture.get().get(0).rows();
        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));
        assertEquals("normal", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().persistentSettings().get(persistentSetting));
        assertEquals("243", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().transientSettings().get(transientSetting));
    }

    @Test
    public void testCreateIndexTemplateTask() throws Exception {
        Settings indexSettings = ImmutableSettings.builder()
                .put("number_of_replicas", 0)
                .put("number_of_shards", 2)
                .build();
        Map<String, Object> mapping = ImmutableMap.<String, Object>of(
                "properties", ImmutableMap.of(
                        "id", ImmutableMap.builder()
                                .put("type", "integer")
                                .put("store", false)
                                .put("index", "not_analyzed")
                                .put("doc_values", true).build(),
                        "name", ImmutableMap.builder()
                                .put("type", "string")
                                .put("store", false)
                                .put("index", "not_analyzed")
                                .put("doc_values", true).build(),
                        "names", ImmutableMap.builder()
                                .put("type", "string")
                                .put("store", false)
                                .put("index", "not_analyzed")
                                .put("doc_values", false).build()
                ),
                "_meta", ImmutableMap.of(
                        "partitioned_by", ImmutableList.<List<String>>of(
                                ImmutableList.of("name", "string")
                        )
                )
        );
        String templateName = PartitionName.templateName("partitioned");
        String templatePrefix = PartitionName.templateName("partitioned") + "*";
        final String alias = "aliasName";

        ESCreateTemplateNode planNode = new ESCreateTemplateNode(
                templateName,
                templatePrefix,
                indexSettings,
                mapping,
                alias);
        Plan plan = new Plan();
        plan.add(planNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> futures = executor.execute(job);
        ListenableFuture<List<TaskResult>> listenableFuture = Futures.allAsList(futures);
        TaskResult taskResult = listenableFuture.get().get(0);
        Object[][] objects = listenableFuture.get().get(0).rows();
        assertThat(taskResult.rowCount(), is(1L));
        assertThat(objects.length, is(0));

        refresh();

        GetIndexTemplatesResponse response = client().admin().indices()
                .prepareGetTemplates(".partitioned.partitioned.").execute().actionGet();

        assertThat(response.getIndexTemplates().size(), is(1));
        IndexTemplateMetaData templateMeta = response.getIndexTemplates().get(0);
        assertThat(templateMeta.getName(), is(".partitioned.partitioned."));
        assertThat(templateMeta.mappings().get(Constants.DEFAULT_MAPPING_TYPE).string(),
                is("{\"default\":" +
                        "{\"properties\":{" +
                        "\"id\":{\"type\":\"integer\",\"store\":false,\"index\":\"not_analyzed\",\"doc_values\":true}," +
                        "\"name\":{\"type\":\"string\",\"store\":false,\"index\":\"not_analyzed\",\"doc_values\":true}," +
                        "\"names\":{\"type\":\"string\",\"store\":false,\"index\":\"not_analyzed\",\"doc_values\":false}" +
                        "}," +
                        "\"_meta\":{" +
                        "\"partitioned_by\":[[\"name\",\"string\"]]" +
                        "}}}"));
        assertThat(templateMeta.template(), is(".partitioned.partitioned.*"));
        assertThat(templateMeta.settings().toDelimitedString(','),
                is("index.number_of_replicas=0,index.number_of_shards=2,"));
        assertThat(templateMeta.aliases().get(alias).alias(), is(alias));
    }
}
