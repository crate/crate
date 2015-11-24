/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.symbol.Reference;
import io.crate.jobs.JobContextService;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportShardUpsertActionTest extends CrateUnitTest {

    private static final TableIdent GENERATED_COLUMN_TABLE_IDENT = new TableIdent(null, "generated_column");
    private static final TestingTableInfo.Builder GENERATED_COLUMN_INFO_BUILDER = new TestingTableInfo.Builder(
            GENERATED_COLUMN_TABLE_IDENT, new Routing(Collections.EMPTY_MAP))
            .add("ts", DataTypes.TIMESTAMP, null)
            .add("user", DataTypes.OBJECT, null)
            .add("user", DataTypes.STRING, Arrays.asList("name"))
            .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", false)
            .addGeneratedColumn("name", DataTypes.STRING, "concat(user['name'], 'bar')", false);
    private static DocTableInfo GENERATED_COLUMN_INFO;

    static class TestingTransportShardUpsertAction extends TransportShardUpsertAction {

        public TestingTransportShardUpsertAction(Settings settings,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 TransportIndexAction indexAction,
                                                 IndicesService indicesService,
                                                 JobContextService jobContextService,
                                                 ShardStateAction shardStateAction,
                                                 Functions functions,
                                                 Schemas schemas) {
            super(settings, threadPool, clusterService, transportService, actionFilters,
                    jobContextService, indexAction, indicesService, shardStateAction, functions, schemas);
        }

        @Override
        protected IndexResponse indexItem(DocTableInfo tableInfo,
                                          ShardUpsertRequest request,
                                          ShardUpsertRequest.Item item,
                                          ShardId shardId,
                                          boolean tryInsertFirst,
                                          int retryCount) throws ElasticsearchException {
            throw new IndexMissingException(new Index(request.index()));
        }
    }

    private TransportShardUpsertAction transportShardUpsertAction;

    @Before
    public void prepare() throws Exception {
        ModulesBuilder builder = new ModulesBuilder();
        builder.add(new ScalarFunctionModule());
        Injector injector = builder.createInjector();
        Functions functions = injector.getInstance(Functions.class);
        if (GENERATED_COLUMN_INFO == null) {
            GENERATED_COLUMN_INFO = GENERATED_COLUMN_INFO_BUILDER.build(injector.getInstance(Functions.class));
        }

        transportShardUpsertAction = new TestingTransportShardUpsertAction(
                ImmutableSettings.EMPTY,
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(TransportService.class),
                mock(ActionFilters.class),
                mock(TransportIndexAction.class),
                mock(IndicesService.class),
                mock(JobContextService.class),
                mock(ShardStateAction.class),
                functions,
                mock(Schemas.class)
                );
    }

    @Test
    public void testIndexMissingExceptionWhileProcessingItemsResultsInFailure() throws Exception {
        TableIdent charactersIdent = new TableIdent(null, "characters");
        final Reference idRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.SHORT));

        ShardId shardId = new ShardId("characters", 0);
        final ShardUpsertRequest request = new ShardUpsertRequest(
                shardId, null, new Reference[]{idRef}, UUID.randomUUID());
        request.add(1, "1", null, new Object[]{1}, null, null);

        ShardUpsertResponse response = transportShardUpsertAction.processRequestItems(
                shardId, request, new AtomicBoolean(false));

        assertThat(response.failures().size(), is(1));
        assertThat(response.failures().get(0).message(), is("IndexMissingException[[characters] missing]"));
    }

    @Test
    public void testProcessGeneratedColumns() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("ts", 1448274317000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns);

        assertThat(updatedColumns.size(), is(2));
        assertThat((Long) updatedColumns.get("day"), is(1448236800000L));
    }

    @Test
    public void testProcessGeneratedColumnsWithValue() throws Exception {
        // just test that passing the correct value will not result in an exception
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("ts", 1448274317000L)
                .put("day", 1448236800000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns);

        assertThat(updatedColumns.size(), is(2));
        assertThat((Long) updatedColumns.get("day"), is(1448236800000L));
    }

    @Test
    public void testProcessGeneratedColumnsWithInvalidValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Given value 1448274317000 for generated column does not match defined generated expression value 1448236800000");

        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("ts", 1448274317000L)
                .put("day", 1448274317000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns);
    }

    @Test
    public void testProcessGeneratedColumnsWithInvalidValueNoValidation() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("ts", 1448274317000L)
                .put("day", 1448274317000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns, false);

        assertThat(updatedColumns.size(), is(2));
        assertThat((Long) updatedColumns.get("day"), is(1448274317000L));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscript() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("user.name", new BytesRef("zoo"))
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("zoobar")));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscriptParentUpdated() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("user", MapBuilder.<String, Object>newMapBuilder().put("name", new BytesRef("zoo")).map())
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("zoobar")));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscriptParentUpdatedValueMissing() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("user", MapBuilder.<String, Object>newMapBuilder().put("age", 35).map())
                .map();

        transportShardUpsertAction.processGeneratedColumns(GENERATED_COLUMN_INFO, updatedColumns);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("bar")));
    }

    @Test
    public void testBuildMapFromSource() throws Exception {
        TableIdent charactersIdent = new TableIdent(null, "characters");
        Reference tsRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(charactersIdent, "ts"), RowGranularity.DOC, DataTypes.TIMESTAMP));
        Reference nameRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(charactersIdent, "user", Arrays.asList("name")), RowGranularity.DOC, DataTypes.TIMESTAMP));


        Reference[] insertColumns = new Reference[]{tsRef, nameRef};
        Object[] insertValues = new Object[]{1448274317000L, "Ford"};

        Map<String, Object> sourceMap = transportShardUpsertAction.buildMapFromSource(insertColumns, insertValues, false);

        validateMapOrder(sourceMap, Arrays.asList("ts", "user.name"));
    }

    @Test
    public void testBuildMapFromRawSource() throws Exception {
        TableIdent charactersIdent = new TableIdent(null, "characters");
        Reference rawRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(charactersIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING));

        BytesRef bytesRef = XContentFactory.jsonBuilder().startObject()
                .field("ts", 1448274317000L)
                .field("user.name", "Ford")
                .endObject()
                .bytes().toBytesRef();

        Reference[] insertColumns = new Reference[]{rawRef};
        Object[] insertValues = new Object[]{bytesRef};

        Map<String, Object> sourceMap = transportShardUpsertAction.buildMapFromSource(insertColumns, insertValues, true);

        validateMapOrder(sourceMap, Arrays.asList("ts", "user.name"));
    }

    private void validateMapOrder(Map<String, Object> map, List<String> keys) {
        assertThat(map, instanceOf(LinkedHashMap.class));

        Iterator<String> it = map.keySet().iterator();
        int idx = 0;
        while (it.hasNext()) {
            String key = it.next();
            assertThat(key, is(keys.get(idx)));
            idx++;
        }

    }
}
