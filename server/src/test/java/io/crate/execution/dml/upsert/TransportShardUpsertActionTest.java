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

package io.crate.execution.dml.upsert;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.TransportAddColumn;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dml.RawIndexer;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.UpsertReplicaRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.netty.NettyBootstrap;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class TransportShardUpsertActionTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationName TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "characters");
    private static final String PARTITION_INDEX = new PartitionName(TABLE_IDENT, List.of("1395874800000")).asIndexName();
    private static final SimpleReference ID_REF = new SimpleReference(
        new ReferenceIdent(TABLE_IDENT, "id"),
        RowGranularity.DOC,
        DataTypes.INTEGER,
        IndexType.PLAIN,
        true,
        false,
        1,
        1,
        false,
        null
    );

    private static final SessionSettings DUMMY_SESSION_INFO = new SessionSettings(
        "dummyUser",
        SearchPath.createSearchPathFrom("dummySchema"));

    private String charactersIndexUUID;
    private String partitionIndexUUID;

    static class TestingTransportShardUpsertAction extends TransportShardUpsertAction {


        public TestingTransportShardUpsertAction(ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 TasksService tasksService,
                                                 IndicesService indicesService,
                                                 ShardStateAction shardStateAction,
                                                 NodeContext nodeCtx) {
            super(
                Settings.EMPTY,
                threadPool,
                clusterService,
                transportService,
                mock(TransportAddColumn.class),
                tasksService,
                indicesService,
                shardStateAction,
                new NoneCircuitBreakerService(),
                nodeCtx);
        }

        @Override
        protected IndexItemResult insert(Indexer indexer,
                                         ShardUpsertRequest request,
                                         IndexItem item,
                                         IndexShard indexShard,
                                         boolean isRetry,
                                         @Nullable RawIndexer rawIndexer,
                                         long version,
                                         long autoGeneratedTimestamp) throws Exception {
            throw new VersionConflictEngineException(
                indexShard.shardId(),
                item.id(),
                "document with id: " + item.id() + " already exists in '" + request.shardId() + '\'');
        }
    }

    private TransportShardUpsertAction transportShardUpsertAction;
    private IndexShard indexShard;
    private NettyBootstrap nettyBootstrap;

    @Before
    public void prepare() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.characters (id int, p int) PARTITIONED BY (p)", PARTITION_INDEX);
        charactersIndexUUID = clusterService.state().metadata().getIndex(TABLE_IDENT, List.of(), true, IndexMetadata::getIndexUUID);
        partitionIndexUUID = clusterService.state().metadata().getIndex(TABLE_IDENT, List.of("1395874800000"), true, IndexMetadata::getIndexUUID);

        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        Index charactersIndex = new Index(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID);
        Index partitionIndex = new Index(PARTITION_INDEX, partitionIndexUUID);

        when(indicesService.indexServiceSafe(charactersIndex)).thenReturn(indexService);
        when(indicesService.indexServiceSafe(partitionIndex)).thenReturn(indexService);
        indexShard = mock(IndexShard.class, Answers.RETURNS_MOCKS);
        when(indexShard.index(any(Engine.Index.class))).thenAnswer(new Answer<>() {

            @Override
            public IndexResult answer(InvocationOnMock arg0) throws Throwable {
                Engine.Index index = arg0.getArgument(0);
                IndexResult indexResult = new IndexResult(1, index.primaryTerm(), index.seqNo(), true);
                indexResult.setTranslogLocation(new Translog.Location(1, 1, 1));
                return indexResult;
            }
        });
        when(indexService.getShard(0)).thenReturn(indexShard);

        // Avoid null pointer exceptions
        DocTableInfo tableInfo = mock(DocTableInfo.class);
        Schemas schemas = mock(Schemas.class);
        when(tableInfo.rootColumns()).thenReturn(Collections.<Reference>emptyList());
        when(tableInfo.versionCreated()).thenReturn(Version.CURRENT);
        when(schemas.getTableInfo(any(RelationName.class))).thenReturn(tableInfo);

        var dynamicLongColRef = new SimpleReference(
                new ReferenceIdent(TABLE_IDENT,"dynamic_long_col"),
                RowGranularity.DOC,
                DataTypes.LONG,
                IndexType.PLAIN,
                true,
                false,
                1,
                1,
                false,
                null
        );
        when(tableInfo.getReference(ColumnIdent.of("dynamic_long_col"))).thenReturn(dynamicLongColRef);
        when(tableInfo.iterator()).thenReturn(List.<Reference>of(ID_REF, dynamicLongColRef).iterator());

        transportShardUpsertAction = new TestingTransportShardUpsertAction(
            mock(ThreadPool.class),
            clusterService,
            MockTransportService.createNewService(Settings.EMPTY, VersionUtils.randomVersion(random()), THREAD_POOL, nettyBootstrap, clusterService.getClusterSettings()),
            mock(TasksService.class),
            indicesService,
            mock(ShardStateAction.class),
            createNodeContext(schemas, List.of())
        );
    }

    @After
    public void teardownNetty() {
        nettyBootstrap.close();
    }

    @Test
    public void testExceptionWhileProcessingItemsNotContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        SimpleReference[] missingAssignmentsColumns = new SimpleReference[]{ID_REF};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            null,
            missingAssignmentsColumns,
            null,
            UUID.randomUUID()
        ).newRequest(shardId);
        request.add(1, ShardUpsertRequest.Item.forInsert(
                "1",
                List.of(),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                missingAssignmentsColumns,
                new Object[]{1},
                null,
                0
            )
        );

        TransportReplicationAction.PrimaryResult<UpsertReplicaRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        assertThat(result.response.failure()).isExactlyInstanceOf(VersionConflictEngineException.class);
    }

    @Test
    public void testExceptionWhileProcessingItemsContinueOnError() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        SimpleReference[] missingAssignmentsColumns = new SimpleReference[]{ID_REF};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            null,
            missingAssignmentsColumns,
            null,
            UUID.randomUUID()
        ).newRequest(shardId);
        request.add(1, ShardUpsertRequest.Item.forInsert(
            "1",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentsColumns,
            new Object[]{1},
            null,
            0
        ));

        TransportReplicationAction.PrimaryResult<UpsertReplicaRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        ShardResponse response = result.response;
        assertThat(response.failures()).satisfiesExactly(
            f -> assertThat(f.error().getMessage()).isEqualTo(
                "[1]: version conflict, document with id: 1 already exists in '[characters/%s][0]'", charactersIndexUUID));
    }

    @Test
    public void testKilledSetWhileProcessingItemsDoesNotThrowException() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        SimpleReference[] missingAssignmentsColumns = new SimpleReference[]{ID_REF};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            null,
            missingAssignmentsColumns,
            null,
            UUID.randomUUID()
        ).newRequest(shardId);
        request.add(1, ShardUpsertRequest.Item.forInsert(
            "1",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentsColumns,
            new Object[]{1},
            null,
            0
        ));

        TransportReplicationAction.PrimaryResult<UpsertReplicaRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(true));

        assertThat(result.response.failure()).isExactlyInstanceOf(InterruptedException.class);
    }


    @Test
    public void test_dynamic_insert_of_integer_upcasted_to_long_can_be_replicated() throws IOException {
        // A dynamic insert to primary creates the dynamic column,
        // so the follow-up dynamic insert to replica is actually no longer dynamic.
        // The follow-up dynamic insert to replica tries to resolve the DynamicReference to a SimpleReference
        // and there can be a potential ClassCastException.
        DynamicReference longCol = new DynamicReference(
            new ReferenceIdent(TABLE_IDENT, ColumnIdent.of("dynamic_long_col")),
            RowGranularity.DOC,
            0
        );
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        String itemId = "1";
        var request = new UpsertReplicaRequest(
            shardId,
            UUID.randomUUID(),
            DUMMY_SESSION_INFO,
            List.of(longCol),
            List.of(
                new UpsertReplicaRequest.Item(
                    itemId,
                    new Object[] { 1 }, // int instead of long
                    List.of(itemId),
                    1,
                    1,
                    1
                )
            )
        );

        // verifies that it does not throw a ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Long
        transportShardUpsertAction.processRequestItemsOnReplica(indexShard, request);
    }

    @Test
    public void test_primary_aborted_remaining_items_must_be_skipped_on_replica() throws IOException {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), charactersIndexUUID, 0);
        SimpleReference[] missingAssignmentsColumns = new SimpleReference[]{ID_REF};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DUMMY_SESSION_INFO,
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            null,
            missingAssignmentsColumns,
            null,
            UUID.randomUUID()
        ).newRequest(shardId);
        request.add(1,
            ShardUpsertRequest.Item.forInsert(
                "1", List.of(), Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                missingAssignmentsColumns,
                new Object[]{1},
                null,
                0));
        request.add(1,
            ShardUpsertRequest.Item.forInsert(
                "2", List.of(), Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                missingAssignmentsColumns,
                new Object[]{2},
                null,
                0));


        // First item is already processed with killed = true, both items must be skipped on replica.
        TransportReplicationAction.PrimaryResult<UpsertReplicaRequest, ShardResponse> result =
            transportShardUpsertAction.processRequestItems(indexShard, request, new AtomicBoolean(true));

        assertThat(result.response.failure()).isExactlyInstanceOf(InterruptedException.class);
        assertThat(result.replicaRequest().items()).isEmpty();
    }
}
