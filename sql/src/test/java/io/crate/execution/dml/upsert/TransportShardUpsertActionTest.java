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

package io.crate.execution.dml.upsert;

import io.crate.Constants;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.jobs.TasksService;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardUpsertActionTest extends CrateDummyClusterServiceUnitTest {

    private final static RelationName TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "characters");
    private final static String PARTITION_INDEX = new PartitionName(TABLE_IDENT, Arrays.asList("1395874800000")).asIndexName();
    private final static Reference ID_REF = new Reference(
        new ReferenceIdent(TABLE_IDENT, "id"), RowGranularity.DOC, DataTypes.SHORT, null);

    private final static SessionSettings DUMMY_SESSION_INFO = new SessionSettings(
        "dummyUser",
        SearchPath.createSearchPathFrom("dummySchema"));

    private String charactersIndexUUID;
    private String partitionIndexUUID;


    static class TestingTransportShardUpsertAction extends TransportShardUpsertAction {


        public TestingTransportShardUpsertAction(Settings settings,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 SchemaUpdateClient schemaUpdateClient,
                                                 TasksService tasksService,
                                                 IndicesService indicesService,
                                                 ShardStateAction shardStateAction,
                                                 Functions functions,
                                                 Schemas schemas,
                                                 IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, threadPool, clusterService, transportService, schemaUpdateClient,
                tasksService, indicesService, shardStateAction, functions, schemas, indexNameExpressionResolver);
        }

        @Nullable
        @Override
        protected Translog.Location indexItem(ShardUpsertRequest request,
                                              ShardUpsertRequest.Item item,
                                              IndexShard indexShard,
                                              boolean tryInsertFirst,
                                              UpdateSourceGen updateSourceGen,
                                              InsertSourceGen insertSourceGen,
                                              boolean isRetry) throws Exception {
             throw new VersionConflictEngineException(
                indexShard.shardId(),
                 Constants.DEFAULT_MAPPING_TYPE,
                item.id(),
                "document with id: " + item.id() + " already exists in '" + request.shardId().getIndexName() + '\'');
        }
    }

    private TransportShardUpsertAction transportShardUpsertAction;
    private IndexShard indexShard;

    @Before
    public void prepare() throws Exception {
        Functions functions = getFunctions();

        charactersIndexUUID = UUIDs.randomBase64UUID();
        partitionIndexUUID = UUIDs.randomBase64UUID();

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        Index charactersIndex = new Index(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID);
        Index partitionIndex = new Index(PARTITION_INDEX, partitionIndexUUID);

        when(indicesService.indexServiceSafe(charactersIndex)).thenReturn(indexService);
        when(indicesService.indexServiceSafe(partitionIndex)).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.getShard(0)).thenReturn(indexShard);

        // Avoid null pointer exceptions
        DocTableInfo tableInfo = mock(DocTableInfo.class);
        Schemas schemas = mock(Schemas.class);
        when(tableInfo.columns()).thenReturn(Collections.<Reference>emptyList());
        when(schemas.getTableInfo(any(RelationName.class), eq(Operation.INSERT))).thenReturn(tableInfo);

        transportShardUpsertAction = new TestingTransportShardUpsertAction(
            Settings.EMPTY,
            mock(ThreadPool.class),
            clusterService,
            MockTransportService.createNewService(Settings.EMPTY, Version.ES_V_6_5_1, THREAD_POOL, clusterService.getClusterSettings()),
            mock(SchemaUpdateClient.class),
            mock(TasksService.class),
            indicesService,
            mock(ShardStateAction.class),
            functions,
            schemas,
            mock(IndexNameExpressionResolver.class)
        );
    }

    @Test
    public void testExceptionWhileProcessingItemsNotContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null, null, null));

        TransportWriteAction.WritePrimaryResult<ShardUpsertRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        assertThat(result.finalResponseIfSuccessful.failure(), instanceOf(VersionConflictEngineException.class));
    }

    @Test
    public void testExceptionWhileProcessingItemsContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null, null, null));

        TransportWriteAction.WritePrimaryResult<ShardUpsertRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        ShardResponse response = result.finalResponseIfSuccessful;
        assertThat(response.failures().size(), is(1));
        assertThat(response.failures().get(0).message(),
            is("[default][1]: version conflict, document with id: 1 already exists in 'characters'"));
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
    public void testKilledSetWhileProcessingItemsDoesNotThrowException() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null, null, null));

        TransportWriteAction.WritePrimaryResult<ShardUpsertRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(true));

        assertThat(result.finalResponseIfSuccessful.failure(), instanceOf(InterruptedException.class));
    }

    @Test
    public void testItemsWithoutSourceAreSkippedOnReplicaOperation() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            null,
            new Reference[]{ID_REF},
            UUID.randomUUID(),
            false
        ).newRequest(shardId);
        request.add(1, new ShardUpsertRequest.Item("1", null, new Object[]{1}, null, null, null));

        reset(indexShard);

        // would fail with NPE if not skipped
        transportShardUpsertAction.processRequestItemsOnReplica(indexShard, request);
        verify(indexShard, times(0)).applyIndexOperationOnReplica(
            anyLong(), anyLong(), anyLong(), anyBoolean(), any(SourceToParse.class));
    }
}
