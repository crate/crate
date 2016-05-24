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
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class TransportShardUpsertActionTest extends CrateUnitTest {

    private DocTableInfo generatedColumnTableInfo;

    private final static TableIdent TABLE_IDENT = new TableIdent(null, "characters");
    private final static String PARTITION_INDEX = new PartitionName(TABLE_IDENT, Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
    private final static Reference ID_REF = new Reference(
            new ReferenceInfo(new ReferenceIdent(TABLE_IDENT, "id"), RowGranularity.DOC, DataTypes.SHORT));


    static class TestingTransportShardUpsertAction extends TransportShardUpsertAction {

        public TestingTransportShardUpsertAction(Settings settings,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 IndicesService indicesService,
                                                 JobContextService jobContextService,
                                                 ShardStateAction shardStateAction,
                                                 Functions functions,
                                                 Schemas schemas,
                                                 MappingUpdatedAction mappingUpdatedAction,
                                                 IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, threadPool, clusterService, transportService, actionFilters,
                    jobContextService, indicesService, shardStateAction, functions, schemas,
                    mappingUpdatedAction, indexNameExpressionResolver);
        }

        @Override
        protected Translog.Location indexItem(DocTableInfo tableInfo,
                                              ShardUpsertRequest request,
                                              ShardUpsertRequest.Item item,
                                              IndexShard indexShard,
                                              boolean tryInsertFirst,
                                              int retryCount) throws ElasticsearchException {
            throw new DocumentAlreadyExistsException(new ShardId(request.index(), request.shardId().id()), request.type(), item.id());
        }
    }

    private TransportShardUpsertAction transportShardUpsertAction;
    private IndexShard indexShard;

    @Before
    public void prepare() throws Exception {
        Functions functions = getFunctions();
        bindGeneratedColumnTable(functions);

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(TABLE_IDENT.indexName())).thenReturn(indexService);
        when(indicesService.indexServiceSafe(PARTITION_INDEX)).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.shardSafe(0)).thenReturn(indexShard);


        transportShardUpsertAction = new TestingTransportShardUpsertAction(
                Settings.EMPTY,
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(TransportService.class),
                mock(ActionFilters.class),
                indicesService,
                mock(JobContextService.class),
                mock(ShardStateAction.class),
                functions,
                mock(Schemas.class),
                mock(MappingUpdatedAction.class),
                mock(IndexNameExpressionResolver.class)
                );
    }

    private void bindGeneratedColumnTable(Functions functions) {
        TableIdent generatedColumnTableIdent = new TableIdent(null, "generated_column");
        generatedColumnTableInfo = new TestingTableInfo.Builder(
                generatedColumnTableIdent, new Routing(Collections.EMPTY_MAP))
                .add("ts", DataTypes.TIMESTAMP, null)
                .add("user", DataTypes.OBJECT, null)
                .add("user", DataTypes.STRING, Arrays.asList("name"))
                .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", false)
                .addGeneratedColumn("name", DataTypes.STRING, "concat(user['name'], 'bar')", false)
                .build(functions);

    }

    @Test
    public void testExceptionWhileProcessingItemsNotContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), 0);
        final ShardUpsertRequest request = new ShardUpsertRequest(
                shardId, null, new Reference[]{ID_REF}, null, UUID.randomUUID());
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        ShardResponse shardResponse = transportShardUpsertAction.processRequestItems(
                shardId, request, new AtomicBoolean(false));

        assertThat(shardResponse.failure(), instanceOf(DocumentAlreadyExistsException.class));
    }

    @Test
    public void testExceptionWhileProcessingItemsContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), 0);
        final ShardUpsertRequest request = new ShardUpsertRequest(
                shardId, null, new Reference[]{ID_REF}, null, UUID.randomUUID());
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));
        request.continueOnError(true);

        ShardResponse response = transportShardUpsertAction.processRequestItems(
                shardId, request, new AtomicBoolean(false));

        assertThat(response.failures().size(), is(1));
        assertThat(response.failures().get(0).message(), is("DocumentAlreadyExistsException[[default][1]: document already exists]"));
    }

    @Test
    public void testProcessGeneratedColumns() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("ts", 1448274317000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, Collections.<String, Object>emptyMap(), true);

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

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, Collections.<String, Object>emptyMap(), true);

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
                .map();

        Map<String, Object> updatedGeneratedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("day", 1448274317000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, updatedGeneratedColumns, true);
    }

    @Test
    public void testProcessGeneratedColumnsWithInvalidValueNoValidation() throws Exception {
        // just test that no exception is thrown even that the value does not match expression value
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("ts", 1448274317000L)
                .map();

        Map<String, Object> updatedGeneratedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("day", 1448274317000L)
                .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, updatedGeneratedColumns, false);
    }

    @Test
    public void testGeneratedColumnsValidationWorksForArrayColumns() throws Exception {
        // test no exception are thrown when validating array generated columns
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
            .put("obj", MapBuilder.<String, Object>newMapBuilder().put("arr", new Object[]{1}).map())
            .map();

        Map<String, Object> updatedGeneratedColumns = MapBuilder.<String, Object>newMapBuilder()
            .put("arr", new Object[]{1})
            .map();

        DocTableInfo docTableInfo = new TestingTableInfo.Builder(
            new TableIdent(null, "generated_column"),
            new Routing(Collections.<String, Map<String, List<Integer>>>emptyMap()))
            .add("obj", DataTypes.OBJECT, null)
            .add("obj", new ArrayType(DataTypes.INTEGER), Arrays.asList("arr"))
            .addGeneratedColumn("arr", new ArrayType(DataTypes.INTEGER), "obj['arr']", false)
            .build(getFunctions());

        transportShardUpsertAction.processGeneratedColumns(docTableInfo, updatedColumns, updatedGeneratedColumns, false);
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscript() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("user.name", new BytesRef("zoo"))
                .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, Collections.<String, Object>emptyMap(), true);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("zoobar")));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscriptParentUpdated() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("user", MapBuilder.<String, Object>newMapBuilder().put("name", new BytesRef("zoo")).map())
                .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, Collections.<String, Object>emptyMap(), true);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("zoobar")));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscriptParentUpdatedValueMissing() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
                .put("user", MapBuilder.<String, Object>newMapBuilder().put("age", 35).map())
                .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns, Collections.<String, Object>emptyMap(), true);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("bar")));
    }

    @Test
    public void testBuildMapFromSource() throws Exception {
        Reference tsRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(TABLE_IDENT, "ts"), RowGranularity.DOC, DataTypes.TIMESTAMP));
        Reference nameRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(TABLE_IDENT, "user", Arrays.asList("name")), RowGranularity.DOC, DataTypes.TIMESTAMP));


        Reference[] insertColumns = new Reference[]{tsRef, nameRef};
        Object[] insertValues = new Object[]{1448274317000L, "Ford"};

        Map<String, Object> sourceMap = transportShardUpsertAction.buildMapFromSource(insertColumns, insertValues, false);

        validateMapOrder(sourceMap, Arrays.asList("ts", "user.name"));
    }

    @Test
    public void testBuildMapFromRawSource() throws Exception {
        Reference rawRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(TABLE_IDENT, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING));

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

    @Test
    public void testUpdateSourceByPathsUpdateNullObject() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("o", null);

        Map<String, Object> changes = new HashMap<>();
        changes.put("o.o", 5);

        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Object o is null, cannot write {o=5} onto it");
        TransportShardUpsertAction.updateSourceByPaths(source, changes);
    }

    @Test
    public void testUpdateSourceByPathsUpdateNullObjectNested() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("o", null);

        Map<String, Object> changes = new HashMap<>();
        changes.put("o.x.y", 5);

        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Object o is null, cannot write {x.y=5} onto it");
        TransportShardUpsertAction.updateSourceByPaths(source, changes);
    }

    @Test
    public void testKilledSetWhileProcessingItemsDoesNotThrowException() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), 0);
        final ShardUpsertRequest request = new ShardUpsertRequest(
            shardId, null, new Reference[]{ID_REF}, null, UUID.randomUUID());
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        ShardResponse shardResponse = transportShardUpsertAction.processRequestItems(
            shardId, request, new AtomicBoolean(true));

        assertThat(shardResponse.failure(), instanceOf(InterruptedException.class));
    }

    @Test
    public void testItemsWithoutSourceAreSkippedOnReplicaOperation() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), 0);
        final ShardUpsertRequest request = new ShardUpsertRequest(
            shardId, null, new Reference[]{ID_REF}, null, UUID.randomUUID());
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        reset(indexShard);

        // would fail with NPE if not skipped
        transportShardUpsertAction.processRequestItemsOnReplica(shardId, request);
        verify(indexShard, times(0)).index(any(Engine.Index.class));
    }
}
