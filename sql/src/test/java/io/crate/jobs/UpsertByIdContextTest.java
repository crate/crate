package io.crate.jobs;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.concurrent.ContextCompletionListener;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ShardResponse;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.planner.node.dml.UpsertByIdNode;
import io.crate.test.CauseMatcher;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.mockito.Mockito.*;

public class UpsertByIdContextTest extends CrateUnitTest {

    @Mock
    public BulkRequestExecutor delegate;

    private UpsertByIdContext context;
    private ContextCompletionListener completionListener;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ShardUpsertRequest request = mock(ShardUpsertRequest.class);
        UpsertByIdNode.Item item = mock(UpsertByIdNode.Item.class);
        context = new UpsertByIdContext(1, request, item, SettableFuture.<TaskResult>create(), delegate);
        completionListener = new ContextCompletionListener();
        context.addListener(completionListener);
    }

    @Test
    public void testKill() throws Exception {
        ArgumentCaptor<ActionListener> listener = ArgumentCaptor.forClass(ActionListener.class);
        context.prepare();
        context.start();
        verify(delegate).execute(any(ShardUpsertRequest.class), listener.capture());

        // context is killed
        context.kill(null);
        // listener returns
        ShardResponse response = mock(ShardResponse.class);
        listener.getValue().onResponse(response);

        expectedException.expectCause(CauseMatcher.cause(InterruptedException.class));
        completionListener.get();
    }

    @Test
    public void testKillBeforeStart() throws Exception {
        context.prepare();
        context.kill(null);
        expectedException.expectCause(CauseMatcher.cause(InterruptedException.class));
        completionListener.get();
    }

    @Test
    public void testStartAfterClose() throws Exception {
        context.prepare();
        context.close();

        context.start();
        // start does nothing, because the context is already closed
        verify(delegate, never()).execute(any(ShardUpsertRequest.class), any(ActionListener.class));
    }
}
