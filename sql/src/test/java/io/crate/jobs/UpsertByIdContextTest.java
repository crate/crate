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
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

public class UpsertByIdContextTest extends CrateUnitTest {

    @Test
    public void testKill() throws Exception {
        SymbolBasedShardUpsertRequest request = mock(SymbolBasedShardUpsertRequest.class);
        SymbolBasedUpsertByIdNode.Item item = mock(SymbolBasedUpsertByIdNode.Item.class);
        SymbolBasedTransportShardUpsertActionDelegate delegate = mock(SymbolBasedTransportShardUpsertActionDelegate.class);

        SettableFuture<TaskResult> future = SettableFuture.create();
        UpsertByIdContext context = new UpsertByIdContext(request, item, future, delegate);
        ContextCallback callback = mock(ContextCallback.class);
        context.addCallback(callback);

        ArgumentCaptor<ActionListener> listener = ArgumentCaptor.forClass(ActionListener.class);
        context.start();
        verify(delegate).execute(any(SymbolBasedShardUpsertRequest.class), listener.capture());

        // context is killed
        context.kill();
        // listener returns
        ShardUpsertResponse response = mock(ShardUpsertResponse.class);
        listener.getValue().onResponse(response);

        verify(callback, times(1)).onClose();
        expectedException.expectCause(TestingHelpers.cause(JobKilledException.class));
        expectedException.expectMessage("Job execution was interrupted by kill");
        future.get();
    }
}
