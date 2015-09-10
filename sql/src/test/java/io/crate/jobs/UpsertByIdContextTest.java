package io.crate.jobs;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.SymbolBasedTransportShardUpsertActionDelegate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class UpsertByIdContextTest extends CrateUnitTest {

    private SymbolBasedTransportShardUpsertActionDelegate delegate;
    private UpsertByIdContext context;
    private SettableFuture<TaskResult> future;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        delegate = mock(SymbolBasedTransportShardUpsertActionDelegate.class);
        SymbolBasedShardUpsertRequest request = mock(SymbolBasedShardUpsertRequest.class);
        SymbolBasedUpsertByIdNode.Item item = mock(SymbolBasedUpsertByIdNode.Item.class);
        future = SettableFuture.create();
        context = new UpsertByIdContext(1, request, item, future, delegate);
    }

    @Test
    public void testKill() throws Exception {
        ArgumentCaptor<ActionListener> listener = ArgumentCaptor.forClass(ActionListener.class);
        context.prepare();
        context.start();
        verify(delegate).execute(any(SymbolBasedShardUpsertRequest.class), listener.capture());

        // context is killed
        context.kill(null);
        // listener returns
        ShardUpsertResponse response = mock(ShardUpsertResponse.class);
        listener.getValue().onResponse(response);

        expectedException.expectCause(TestingHelpers.cause(CancellationException.class));
        context.future().get(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testKillBeforeStart() throws Exception {
        context.prepare();
        context.kill(null);
        expectedException.expectCause(TestingHelpers.cause(CancellationException.class));
        context.future().get(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testStartAfterClose() throws Exception {
        context.prepare();
        context.close();

        context.start();
        // start does nothing, because the context is already closed
        verify(delegate, never()).execute(any(SymbolBasedShardUpsertRequest.class), any(ActionListener.class));
    }
}
