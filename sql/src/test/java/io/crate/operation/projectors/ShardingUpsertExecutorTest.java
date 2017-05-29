package io.crate.operation.projectors;


import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.*;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportShardUpsertAction;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.collect.RowShardResolver;
import io.crate.testing.TestingBatchConsumer;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.doThrow;


public class ShardingUpsertExecutorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent O_IDENT = new ColumnIdent("o");
    private static final TableIdent tIdent = new TableIdent(null, "t");
    private static ShardingUpsertExecutor shardingUpsertExecutor;
    private static ScheduledExecutorService scheduler;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.bulk.queue_size", 1)
            .put("thread_pool.bulk.size", 1)
            .build();
    }

    @Before
    void prepareTest() {
        InputCollectExpression sourceInput = new InputCollectExpression(1);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);
        UUID jobID = UUID.randomUUID();
        Functions functions = internalCluster().getInstance(Functions.class);
        List<ColumnIdent> primaryKeyIdents = Arrays.asList(O_IDENT);
        List<? extends Symbol> primaryKeySymbols = Arrays.<Symbol>asList(new InputColumn(0));
        RowShardResolver rowShardResolver = new RowShardResolver(functions, primaryKeyIdents, primaryKeySymbols, null, null);

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(Settings.EMPTY),
            false,
            true,
            null,
            new Reference[]{new Reference(new ReferenceIdent(tIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING)},
            jobID,
            false);

        Function<String, ShardUpsertRequest.Item> itemFactory = id ->
            new ShardUpsertRequest.Item(id, null, new Object[]{sourceInput.value()}, null);


        scheduler = internalCluster().getInstance(ThreadPool.class).scheduler();
        shardingUpsertExecutor = new ShardingUpsertExecutor<>(
            internalCluster().getInstance(ClusterService.class),
            new NodeJobsCounter(),
            scheduler,
            1,
            jobID,
            rowShardResolver,
            itemFactory,
            builder::newRequest,
            collectExpressions,
            IndexNameResolver.forTable(new TableIdent(null, "t")),
            false,
            internalCluster().getInstance(TransportShardUpsertAction.class)::execute,
            internalCluster().getInstance(TransportBulkCreateIndicesAction.class));
    }

    @Test
    public void testShardingUpsertExecutorWithLimitedResources() throws Throwable {
        execute("create table t (o int) with (number_of_replicas=0)");
        ensureGreen();

        BatchIterator rowsIterator = RowsBatchIterator.newInstance(IntStream.range(0, 100)
            .mapToObj(i -> new RowN(new Object[]{i, new BytesRef("{\"o\": " + i + "}")}))
            .collect(Collectors.toList()), 2);

        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(CollectingBatchIterator.newInstance(rowsIterator, shardingUpsertExecutor, 1), null);
        Bucket objects = consumer.getBucket();

        assertThat(objects, contains(isRow(100L)));

        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(100L));
    }

    @Test
    public void testShardingUpsertExecutorSchedulerException() throws Throwable {
        execute("create table t (o int) with (number_of_replicas=0)");
        ensureGreen();
        ShardingUpsertExecutor shardingUpsertExecutorSpy = Mockito.spy(shardingUpsertExecutor);


        BatchIterator rowsIterator = RowsBatchIterator.newInstance(IntStream.range(0, 2)
            .mapToObj(i -> new RowN(new Object[]{i, new BytesRef("{\"o\": " + i + "}")}))
            .collect(Collectors.toList()), 1);


        Mockito.doReturn(1).when(shardingUpsertExecutorSpy).getBulkSize();
        Mockito.doReturn(1).when(shardingUpsertExecutorSpy).getIndexInBulk();
        Mockito.doReturn(false).when(shardingUpsertExecutorSpy).tryExecuteBulk(rowsIterator, false);
        Mockito.doReturn(true).when(shardingUpsertExecutorSpy).compareAndSet(false, true);


        doThrow(new EsRejectedExecutionException()).doNothing().when(shardingUpsertExecutorSpy).scheduleConsumeIterator(rowsIterator);

        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(CollectingBatchIterator.newInstance(rowsIterator, shardingUpsertExecutorSpy, 1), null);

        //verify unsafeConsumeIterator is called at least once
        Mockito.verify(shardingUpsertExecutorSpy, Mockito.atLeast(1)).unsafeConsumeIterator(rowsIterator);
        //verify shardingUpsertExecutorSpy is called at least twice
        // the first call will throw an exception
        // the following calls won't do anything
        Mockito.verify(shardingUpsertExecutorSpy, Mockito.atLeast(2)).scheduleConsumeIterator(rowsIterator);
        scheduler.shutdownNow();
    }

}
