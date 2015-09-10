package io.crate.jobs;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.operation.PageDownstream;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.elasticsearch.action.bulk.SymbolBasedTransportShardUpsertActionDelegate;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.TransportAction;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionSubContextTest extends CrateUnitTest {

    private void verifyParallel(final ExecutionSubContext subContext, final boolean kill) throws Throwable {
        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        final AtomicInteger closed = new AtomicInteger(0);
        final AtomicInteger killed = new AtomicInteger(0);
        final ContextCallback callback = new ContextCallback() {


            @Override
            public void onClose(@Nullable Throwable error, long bytesUsed) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                closed.incrementAndGet();
            }

            @Override
            public void onKill() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                killed.incrementAndGet();
            }

            @Override
            public void keepAlive() {

            }
        };
        subContext.addCallback(callback);
        subContext.prepare();
        subContext.start();
        Runnable closeAndVerify = new Runnable() {
            @Override
            public void run() {
                if (kill) {
                    subContext.kill(null);
                    if (killed.get() != 1) {
                        throwable.set(new AssertionError("Callback.onKill() should be called 1 time. Actual: " + killed.get()));
                    }
                } else {
                    subContext.close();
                    if (closed.get() != 1) {
                        throwable.set(new AssertionError("Callback.onClose() should be called 1 time. Actual: " + closed.get()));
                    }
                }
            }
        };

        Thread close1 = new Thread(closeAndVerify);
        Thread close2 = new Thread(closeAndVerify);
        close1.start();
        close2.start();

        close1.join();
        close2.join();
        if (throwable.get() != null) {
            throw throwable.get();
        }
    }

    private void verifyParallelClose(ExecutionSubContext subContext) throws Throwable {
        verifyParallel(subContext, false);
    }

    private void verifyParallelKill(ExecutionSubContext subContext) throws Throwable {
        verifyParallel(subContext, true);
    }


    @Test
    public void testParallelCloseJobCollectContext() throws Throwable {
        verifyParallelClose(new JobCollectContext(
                UUID.randomUUID(),
                mock(CollectPhase.class),
                mock(MapSideDataCollectOperation.class),
                mock(RamAccountingContext.class),
                new CollectingRowReceiver()
        ));
    }

    @Test
    public void testParallelKillJobCollectContext() throws Throwable {
        verifyParallelKill(new JobCollectContext(
                UUID.randomUUID(),
                mock(CollectPhase.class),
                mock(MapSideDataCollectOperation.class),
                mock(RamAccountingContext.class),
                new CollectingRowReceiver()
        ));
    }

    private CountContext createCountContext() throws Throwable {
        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(anyMap(), any(WhereClause.class))).thenReturn(SettableFuture.<Long>create());
        return new CountContext(countOperation, new CollectingRowReceiver(), null, WhereClause.MATCH_ALL);
    }

    @Test
    public void testParallelCloseCountContext() throws Throwable {
        verifyParallelClose(createCountContext());
    }

    @Test
    public void testParallelKillCountContext() throws Throwable {
        verifyParallelKill(createCountContext());
    }

    @Test
    public void testParallelCloseESJobContext() throws Throwable {
        verifyParallelClose(new ESJobContext("delete",
                new ArrayList<DeleteRequest>(), null, null,
                mock(TransportAction.class), null));
    }

    @Test
    public void testParallelKillESJobContext() throws Throwable {
        verifyParallelKill(new ESJobContext("delete",
                new ArrayList<DeleteRequest>(), null, new ArrayList<Future<TaskResult>>(),
                mock(TransportAction.class), null));
    }

    @Test
    public void testParallelClosePageDownstreamContext() throws Throwable {
        verifyParallelClose(new PageDownstreamContext("dummy",
                mock(PageDownstream.class),
                new Streamer[0],
                mock(RamAccountingContext.class),
                3, mock(FlatProjectorChain.class)));
    }

    @Test
    public void testParallelKillPageDownstreamContext() throws Throwable {
        verifyParallelKill(new PageDownstreamContext("dummy",
                mock(PageDownstream.class),
                new Streamer[0],
                mock(RamAccountingContext.class),
                3, mock(FlatProjectorChain.class)));
    }

    @Test
    public void testParallelCloseSymbolBasedBulkShardProcessorContext() throws Throwable {
        verifyParallelClose(new SymbolBasedBulkShardProcessorContext(mock(SymbolBasedBulkShardProcessor.class)));
    }

    @Test
    public void testParallelKillSymbolBasedBulkShardProcessorContext() throws Throwable {
        verifyParallelKill(new SymbolBasedBulkShardProcessorContext(mock(SymbolBasedBulkShardProcessor.class)));
    }

    @Test
    public void testParallelCloseUpsertByIdContext() throws Throwable {
        verifyParallelClose(new UpsertByIdContext(mock(SymbolBasedShardUpsertRequest.class),
                mock(SymbolBasedUpsertByIdNode.Item.class),
                SettableFuture.<TaskResult>create(),
                mock(SymbolBasedTransportShardUpsertActionDelegate.class)));
    }

    @Test
    public void testParallelKillUpsertByIdContext() throws Throwable {
        verifyParallelKill(new UpsertByIdContext(mock(SymbolBasedShardUpsertRequest.class),
                mock(SymbolBasedUpsertByIdNode.Item.class),
                SettableFuture.<TaskResult>create(),
                mock(SymbolBasedTransportShardUpsertActionDelegate.class)));
    }
}
