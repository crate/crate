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

import static org.mockito.Mockito.*;

public class UpsertByIdContextTest extends CrateUnitTest {

    private SymbolBasedTransportShardUpsertActionDelegate delegate;
    private UpsertByIdContext context;
    private SettableFuture<TaskResult> future;
    private ContextCallback callback;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        delegate = mock(SymbolBasedTransportShardUpsertActionDelegate.class);
        SymbolBasedShardUpsertRequest request = mock(SymbolBasedShardUpsertRequest.class);
        SymbolBasedUpsertByIdNode.Item item = mock(SymbolBasedUpsertByIdNode.Item.class);
        future = SettableFuture.create();
        context = new UpsertByIdContext(request, item, future, delegate);

        callback = mock(ContextCallback.class);
        context.addCallback(callback);
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

        verify(callback, times(1)).onKill();
        expectedException.expectCause(TestingHelpers.cause(CancellationException.class));
        future.get();
    }

    @Test
    public void testStartAfterKill() throws Exception {
        context.prepare();
        context.kill(null);
        verify(callback, times(1)).onKill();
        expectedException.expectCause(TestingHelpers.cause(CancellationException.class));
        future.get();

        // start does nothing, because the context is already closed
        context.start();
        verify(delegate, never()).execute(any(SymbolBasedShardUpsertRequest.class), any(ActionListener.class));

        // close context and verify that the callbacks are not called twice
        context.close();
        verify(callback, times(1)).onClose(any(Throwable.class), anyLong());
    }

    @Test
    public void testStartAfterClose() throws Exception {
        context.prepare();
        context.close();
        verify(callback, times(1)).onClose(any(Throwable.class), anyLong());

        context.start();
        expectedException.expectCause(TestingHelpers.cause(ContextClosedException.class));
        expectedException.expectMessage("Can't start already closed context");
        future.get();
        // start does nothing, because the context is already closed
        verify(delegate, never()).execute(any(SymbolBasedShardUpsertRequest.class), any(ActionListener.class));

        // kill context to verify that the callbacks are not called twice
        context.kill(null);
        verify(callback, times(1)).onClose(any(Throwable.class), anyLong());
        verify(callback, times(0)).onKill();
    }
}
