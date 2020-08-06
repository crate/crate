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

package io.crate.operation.aggregation;

import io.crate.Constants;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccounting;
import io.crate.common.collections.Lists2;
import io.crate.common.io.IOUtils;
import io.crate.data.ArrayBucket;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.DocValuesAggregates;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.engine.collect.MapSideDataCollectOperation;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.memory.MemoryManager;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.crate.metadata.RelationName.fromIndexName;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_RECOVERY;
import static org.elasticsearch.index.shard.IndexShardTestCase.EMPTY_EVENT_LISTENER;
import static org.elasticsearch.index.translog.Translog.UNSET_AUTO_GENERATED_TIMESTAMP;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AggregationTest extends CrateUnitTest {

    protected static final RamAccounting RAM_ACCOUNTING = RamAccounting.NO_ACCOUNTING;

    protected Functions functions;
    protected MemoryManager memoryManager;
    private ThreadPool threadPool;

    private IndicesModule indicesModule;
    private IndicesService indexServices;
    private IndexService indexService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        functions = getFunctions();
        threadPool = new TestThreadPool(getClass().getName(), Settings.EMPTY);
        memoryManager = new OnHeapMemoryManager(RAM_ACCOUNTING::addBytes);
        indicesModule = new IndicesModule(List.of(new MapperPlugin() {
            @Override
            public Map<String, Mapper.TypeParser> getMappers() {
                return Map.of(ArrayMapper.CONTENT_TYPE, new ArrayTypeParser());
            }
        }));
        indexServices = mock(IndicesService.class);
        indexService = mock(IndexService.class);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        } finally {
            super.tearDown();
        }
    }

    public Object executeAggregation(Signature boundSignature,
                                     Object[][] data) throws Exception {
        return executeAggregation(
            boundSignature,
            Lists2.map(boundSignature.getArgumentTypes(), TypeSignature::createType),
            boundSignature.getReturnType().createType(),
            data
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object executeAggregation(Signature maybeUnboundSignature,
                                     List<DataType<?>> actualArgumentTypes,
                                     DataType<?> actualReturnType,
                                     Object[][] data) throws Exception {
        var aggregationFunction = (AggregationFunction) functions.get(
            null,
            maybeUnboundSignature.getName().name(),
            Lists2.map(actualArgumentTypes, t -> new InputColumn(0, t)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        Object partialResultWithoutDocValues = execPartialAggregationWithoutDocValues(
            aggregationFunction,
            data
        );

        var shard = newStartedPrimaryShard(Settings.EMPTY, buildMapping(actualArgumentTypes));
        when(indexService.getShard(shard.shardId().id()))
            .thenReturn(shard);
        when(indexServices.indexServiceSafe(shard.routingEntry().index()))
            .thenReturn(indexService);
        try {
            insertDataIntoShard(shard, data);
            shard.refresh("test");

            List<Row> partialResultWithDocValues = execPartialAggregationWithDocValues(
                maybeUnboundSignature,
                actualArgumentTypes,
                actualReturnType,
                shard
            );
            // assert that aggregations with/-out doc values yield the
            // same result, if a doc value aggregator exists.
            if (partialResultWithDocValues != null) {
                assertThat(partialResultWithDocValues.size(), is(1));
                assertThat(
                    partialResultWithoutDocValues,
                    is(partialResultWithDocValues.get(0).get(0))
                );
            }
        } finally {
            closeShard(shard);
        }
        return aggregationFunction.terminatePartial(
            RAM_ACCOUNTING,
            partialResultWithoutDocValues
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object execPartialAggregationWithoutDocValues(AggregationFunction function,
                                                          Object[][] data) {
        var argumentsSize = function.signature().getArgumentTypes().size();
        InputCollectExpression[] inputs = new InputCollectExpression[argumentsSize];
        for (int i = 0; i < argumentsSize; i++) {
            inputs[i] = new InputCollectExpression(i);
        }

        ArrayList<Object> states = new ArrayList<>();
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        states.add(function.newState(RAM_ACCOUNTING, Version.CURRENT, minNodeVersion, memoryManager));
        for (Row row : new ArrayBucket(data)) {
            for (InputCollectExpression input : inputs) {
                input.setNextRow(row);
            }
            if (randomIntBetween(1, 4) == 1) {
                states.add(function.newState(RAM_ACCOUNTING, Version.CURRENT, minNodeVersion, memoryManager));
            }
            int idx = states.size() - 1;
            states.set(idx, function.iterate(RAM_ACCOUNTING, memoryManager, states.get(idx), inputs));
        }
        Object state = states.get(0);
        for (int i = 1; i < states.size(); i++) {
            state = function.reduce(RAM_ACCOUNTING, state, states.get(i));
        }
        return state;
    }

    @Nullable
    private List<Row> execPartialAggregationWithDocValues(Signature signature,
                                                          List<DataType<?>> argumentTypes,
                                                          DataType<?> actualReturnType,
                                                          IndexShard shard) throws Exception {
        var aggregation = new Aggregation(
            signature,
            actualReturnType,
            InputColumn.mapToInputColumns(argumentTypes)
        );
        var toCollectRefs = new ArrayList<Symbol>(argumentTypes.size());
        for (int i = 0; i < argumentTypes.size(); i++) {
            toCollectRefs.add(
                new Reference(
                    new ReferenceIdent(
                        fromIndexName(shard.routingEntry().getIndexName()),
                        Integer.toString(i)),
                    RowGranularity.DOC,
                    argumentTypes.get(i),
                    null,
                    null)
            );
        }
        var collectPhase = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(Map.of()),
            RowGranularity.SHARD,
            toCollectRefs,
            List.of(
                new AggregationProjection(
                    List.of(aggregation),
                    RowGranularity.SHARD,
                    AggregateMode.ITER_PARTIAL
                )
            ),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
        var collectTask = new CollectTask(
            collectPhase,
            CoordinatorTxnCtx.systemTransactionContext(),
            mock(MapSideDataCollectOperation.class),
            RamAccounting.NO_ACCOUNTING,
            ramAccounting -> memoryManager,
            new TestingRowConsumer(),
            new SharedShardContexts(indexServices, UnaryOperator.identity()),
            Version.CURRENT,
            4096);

        var batchIterator = DocValuesAggregates.tryOptimize(
            functions,
            shard,
            mock(DocTableInfo.class),
            new LuceneQueryBuilder(functions),
            shard.mapperService()::fullName,
            collectPhase,
            collectTask
        );
        List<Row> result;
        if (batchIterator != null) {
            result = BatchIterators.collect(batchIterator, Collectors.toList()).get();
        } else {
            result = null;
        }
        // required to close searchers
        collectTask.kill(new InterruptedException("kill"));
        return result;

    }

    private void insertDataIntoShard(IndexShard shard,
                                     Object[][] data) throws IOException {
        for (int i = 0; i < data.length; i++) {
            var cell = data[i];
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (int j = 0; j < cell.length; j++) {
                builder.field(Integer.toString(j), cell[j]);
            }
            builder.endObject();

            var result = shard.applyIndexOperationOnPrimary(
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                new SourceToParse(
                    shard.shardId().getIndexName(),
                    Integer.toString(i),
                    new BytesArray(Strings.toString(builder)),
                    XContentType.JSON,
                    null
                ),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0,
                UNSET_AUTO_GENERATED_TIMESTAMP,
                false);
            assertThat(result.getResultType(), is(Engine.Result.Type.SUCCESS));
        }
    }

    private XContentBuilder buildMapping(List<DataType<?>> argumentTypes) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties");
        for (int i = 0; i < argumentTypes.size(); i++) {
            var type = argumentTypes.get(i);
            builder
                .startObject(Integer.toString(i));
            if (DataTypes.isArray(type)) {
                builder
                    .field("type", "array")
                    .startObject("inner")
                    .field(
                        "type",
                        DataTypes.esMappingNameFrom(
                            ((ArrayType<?>) type).innerType().id()))
                    .endObject();
            } else {
                builder
                    .field("type", DataTypes.esMappingNameFrom(type.id()));
            }
            builder.endObject();
        }
        return builder.endObject().endObject();
    }

    /**
     * Creates a new empty primary shard and starts it.
     */
    private IndexShard newStartedPrimaryShard(Settings settings,
                                              XContentBuilder mapping) throws IOException {
        IndexShard shard = newPrimaryShard(settings, mapping);
        shard.markAsRecovering(
            "store",
            new RecoveryState(
                shard.routingEntry(),
                new DiscoveryNode(
                    shard.routingEntry().currentNodeId(),
                    shard.routingEntry().currentNodeId(),
                    buildNewFakeTransportAddress(),
                    Map.of(),
                    EnumSet.allOf(DiscoveryNode.Role.class),
                    Version.CURRENT),
                null)
        );
        shard.recoverFromStore();

        var newRouting = ShardRoutingHelper.moveToStarted(shard.routingEntry());
        var newRoutingTable = new IndexShardRoutingTable.Builder(newRouting.shardId())
            .addShard(newRouting)
            .build();
        assert newRouting.allocationId() != null;
        shard.updateShardState(
            newRouting,
            shard.getPendingPrimaryTerm(),
            null,
            0,
            Set.of(newRouting.allocationId().getId()),
            newRoutingTable
        );
        return shard;
    }

    /**
     * Creates a new initializing primary shard.
     * The shard will have its own unique data path.
     */
    private IndexShard newPrimaryShard(Settings settings, XContentBuilder mapping) throws IOException {
        ShardRouting routing = TestShardRouting.newShardRouting(
            new ShardId("index", "_na_", 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(settings)
            .build();
        var indexMetadata = IndexMetadata.builder(routing.getIndexName())
            .settings(indexSettings)
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, Strings.toString(mapping))
            .build();
        var shardId = routing.shardId();
        var nodePath = new NodeEnvironment.NodePath(createTempDir());
        var shardPath = new ShardPath(
            false,
            nodePath.resolve(shardId),
            nodePath.resolve(shardId),
            shardId
        );
        return newPrimaryShard(routing, shardPath, indexMetadata);
    }

    private IndexShard newPrimaryShard(ShardRouting routing,
                                       ShardPath shardPath,
                                       IndexMetadata indexMetadata) throws IOException {
        var indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        var indexCache = new IndexCache(
            indexSettings,
            new DisabledQueryCache(indexSettings)
        );
        var store = new Store(
            shardPath.getShardId(),
            indexSettings,
            newFSDirectory(shardPath.resolveIndex()),
            new DummyShardLock(shardPath.getShardId())
        );
        var mapperService = MapperTestUtils.newMapperService(
            xContentRegistry(),
            createTempDir(),
            indexSettings.getSettings(),
            indicesModule,
            routing.getIndexName()
        );
        mapperService.merge(indexMetadata, MAPPING_RECOVERY, true);
        IndexShard shard = null;
        try {
            shard = new IndexShard(
                routing,
                indexSettings,
                shardPath,
                store,
                indexCache,
                mapperService,
                new InternalEngineFactory(),
                EMPTY_EVENT_LISTENER,
                null,
                threadPool,
                BigArrays.NON_RECYCLING_INSTANCE,
                List.of(),
                () -> { },
                RetentionLeaseSyncer.EMPTY,
                new NoneCircuitBreakerService()
            );
        } catch (IOException e) {
            IOUtils.close(store);
            fail("cannot create a new shard");
        }
        return shard;
    }

    private void closeShard(IndexShard shard) throws IOException {
        IOUtils.close(() -> shard.close("test", false), shard.store());
    }

    protected Symbol normalize(String functionName, Object value, DataType type) {
        return normalize(functionName, Literal.of(type, value));
    }

    protected Symbol normalize(String functionName, Symbol... args) {
        List<Symbol> arguments = Arrays.asList(args);
        AggregationFunction function = (AggregationFunction) functions.get(
            null,
            functionName,
            arguments,
            SearchPath.pathWithPGCatalogAndDoc()
        );
        return function.normalizeSymbol(
            new Function(function.signature(), arguments, function.partialType()),
            new CoordinatorTxnCtx(SessionContext.systemSessionContext())
        );
    }
}
