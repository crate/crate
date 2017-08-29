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

import io.crate.exceptions.InvalidColumnNameException;
import io.crate.jobs.JobContextService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardUpsertActionTest extends CrateDummyClusterServiceUnitTest {

    private DocTableInfo generatedColumnTableInfo;

    private final static TableIdent TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "characters");
    private final static String PARTITION_INDEX = new PartitionName(TABLE_IDENT, Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
    private final static Reference ID_REF = new Reference(
        new ReferenceIdent(TABLE_IDENT, "id"), RowGranularity.DOC, DataTypes.SHORT);

    private String charactersIndexUUID;
    private String partitionIndexUUID;


    static class TestingTransportShardUpsertAction extends TransportShardUpsertAction {


        public TestingTransportShardUpsertAction(Settings settings,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 SchemaUpdateClient schemaUpdateClient,
                                                 ActionFilters actionFilters,
                                                 JobContextService jobContextService,
                                                 IndicesService indicesService,
                                                 ShardStateAction shardStateAction,
                                                 Functions functions,
                                                 Schemas schemas,
                                                 IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, threadPool, clusterService, transportService, schemaUpdateClient, actionFilters,
                jobContextService, indicesService, shardStateAction, functions, schemas, indexNameExpressionResolver);
        }

        @Override
        protected Translog.Location indexItem(DocTableInfo tableInfo,
                                              ShardUpsertRequest request,
                                              ShardUpsertRequest.Item item,
                                              IndexShard indexShard,
                                              boolean tryInsertFirst,
                                              Collection<ColumnIdent> notUsedNonGeneratedColumns,
                                              int retryCount) throws ElasticsearchException {
            throw new VersionConflictEngineException(
                indexShard.shardId(),
                request.type(),
                item.id(),
                "document with id: " + item.id() + " already exists in '" + request.shardId().getIndexName() + '\'');
        }
    }

    private TransportShardUpsertAction transportShardUpsertAction;
    private IndexShard indexShard;

    @Before
    public void prepare() throws Exception {
        Functions functions = getFunctions();
        bindGeneratedColumnTable(functions);

        charactersIndexUUID = UUIDs.randomBase64UUID();
        partitionIndexUUID = UUIDs.randomBase64UUID();

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        Index charactersIndex = new Index(TABLE_IDENT.indexName(), charactersIndexUUID);
        Index partitionIndex = new Index(PARTITION_INDEX, partitionIndexUUID);

        when(indicesService.indexServiceSafe(charactersIndex)).thenReturn(indexService);
        when(indicesService.indexServiceSafe(partitionIndex)).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.getShard(0)).thenReturn(indexShard);

        // Avoid null pointer exceptions
        DocTableInfo tableInfo = mock(DocTableInfo.class);
        Schemas schemas = mock(Schemas.class);
        when(tableInfo.columns()).thenReturn(Collections.<Reference>emptyList());
        when(schemas.getTableInfo(any(TableIdent.class), eq(Operation.INSERT))).thenReturn(tableInfo);

        transportShardUpsertAction = new TestingTransportShardUpsertAction(
            Settings.EMPTY,
            mock(ThreadPool.class),
            clusterService,
            MockTransportService.local(Settings.EMPTY, Version.V_5_0_1, THREAD_POOL, clusterService.getClusterSettings()),
            mock(SchemaUpdateClient.class),
            mock(ActionFilters.class),
            mock(JobContextService.class),
            indicesService,
            mock(ShardStateAction.class),
            functions,
            schemas,
            mock(IndexNameExpressionResolver.class)
        );
    }

    private void bindGeneratedColumnTable(Functions functions) {
        TableIdent generatedColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "generated_column");
        generatedColumnTableInfo = new TestingTableInfo.Builder(
            generatedColumnTableIdent, new Routing(Collections.EMPTY_MAP))
            .add("ts", DataTypes.TIMESTAMP, null)
            .add("user", DataTypes.OBJECT, null)
            .add("user", DataTypes.STRING, Arrays.asList("name"))
            .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", false)
            .addGeneratedColumn("name", DataTypes.STRING, "concat(\"user\"['name'], 'bar')", false)
            .build(functions);

    }

    @Test
    public void testExceptionWhileProcessingItemsNotContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            false,
            false,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId, null);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        TransportWriteAction.WritePrimaryResult<ShardUpsertRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        assertThat(result.finalResponseIfSuccessful.failure(), instanceOf(VersionConflictEngineException.class));
    }

    @Test
    public void testExceptionWhileProcessingItemsContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            false,
            true,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId, null);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        TransportWriteAction.WritePrimaryResult<ShardUpsertRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        ShardResponse response = result.finalResponseIfSuccessful;
        assertThat(response.failures().size(), is(1));
        assertThat(response.failures().get(0).message(),
            is("VersionConflictEngineException[[default][1]: version conflict, " +
               "document with id: 1 already exists in 'characters']"));
    }

    @Test
    public void testProcessGeneratedColumns() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
            .put("ts", 1448274317000L)
            .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            Collections.emptyMap(), true, null);

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

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            Collections.emptyMap(), true, null);

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

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            updatedGeneratedColumns, true, null);
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

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            updatedGeneratedColumns, false, null);
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
            new TableIdent(Schemas.DOC_SCHEMA_NAME, "generated_column"),
            new Routing(Collections.<String, Map<String, List<Integer>>>emptyMap()))
            .add("obj", DataTypes.OBJECT, null)
            .add("obj", new ArrayType(DataTypes.INTEGER), Arrays.asList("arr"))
            .addGeneratedColumn("arr", new ArrayType(DataTypes.INTEGER), "obj['arr']", false)
            .build(getFunctions());

        transportShardUpsertAction.processGeneratedColumns(docTableInfo, updatedColumns, updatedGeneratedColumns,
            false, null);
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscript() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
            .put("user.name", new BytesRef("zoo"))
            .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            Collections.emptyMap(), true, null);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("zoobar")));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscriptParentUpdated() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
            .put("user", MapBuilder.<String, Object>newMapBuilder().put("name", new BytesRef("zoo")).map())
            .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            Collections.emptyMap(), true, null);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("zoobar")));
    }

    @Test
    public void testProcessGeneratedColumnsWithSubscriptParentUpdatedValueMissing() throws Exception {
        Map<String, Object> updatedColumns = MapBuilder.<String, Object>newMapBuilder()
            .put("user", MapBuilder.<String, Object>newMapBuilder().put("age", 35).map())
            .map();

        transportShardUpsertAction.processGeneratedColumns(generatedColumnTableInfo, updatedColumns,
            Collections.emptyMap(), true, null);

        assertThat(updatedColumns.size(), is(2));
        assertThat((BytesRef) updatedColumns.get("name"), is(new BytesRef("bar")));
    }

    @Test
    public void testBuildMapFromSource() throws Exception {
        Reference tsRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, "ts"), RowGranularity.DOC, DataTypes.TIMESTAMP);
        Reference nameRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, "user", Arrays.asList("name")), RowGranularity.DOC, DataTypes.TIMESTAMP);


        Reference[] insertColumns = new Reference[]{tsRef, nameRef};
        Object[] insertValues = new Object[]{1448274317000L, "Ford"};

        Map<String, Object> sourceMap = transportShardUpsertAction.buildMapFromSource(insertColumns, insertValues, false);

        validateMapOrder(sourceMap, Arrays.asList("ts", "user.name"));
    }

    @Test
    public void testBuildMapFromRawSource() throws Exception {
        Reference rawRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING);

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
    public void testValidateMapping() throws Exception {
        // Create valid nested mapping with underscore.
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(settings, new ContentPath());
        Mapper outerMapper = new ObjectMapper.Builder("valid")
            .add(new ObjectMapper.Builder("_invalid"))
            .build(builderContext);
        TransportShardUpsertAction.validateMapping(Arrays.asList(outerMapper).iterator(), false);

        // Create invalid mapping
        expectedException.expect(InvalidColumnNameException.class);
        expectedException.expectMessage("system column pattern");
        outerMapper = new ObjectMapper.Builder("_invalid").build(builderContext);
        TransportShardUpsertAction.validateMapping(Arrays.asList(outerMapper).iterator(), false);
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
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            false,
            false,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId, null);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        TransportWriteAction.WritePrimaryResult<ShardUpsertRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(true));

        assertThat(result.finalResponseIfSuccessful.failure(), instanceOf(InterruptedException.class));
    }

    @Test
    public void testItemsWithoutSourceAreSkippedOnReplicaOperation() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            false,
            false,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId, null);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null));

        reset(indexShard);

        // would fail with NPE if not skipped
        transportShardUpsertAction.processRequestItemsOnReplica(indexShard, request);
        verify(indexShard, times(0)).index(any(Engine.Index.class));
    }
}
