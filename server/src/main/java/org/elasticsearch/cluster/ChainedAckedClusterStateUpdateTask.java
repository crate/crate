package org.elasticsearch.cluster;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class ChainedAckedClusterStateUpdateTask extends AckedClusterStateUpdateTask {

    private final String hintOnError;
    private final Function<ClusterState, ClusterState> task;
    private final ClusterService clusterService;
    private final CompletableFuture future;

    public ChainedAckedClusterStateUpdateTask(Priority priority,
                                              AckedRequest request,
                                              CompletableFuture future,
                                              ClusterService clusterService,
                                              Function<ClusterState, ClusterState> task,
                                              String hintOnError) {
        // Parent class AckedClusterStateUpdateTask uses listener in onAllNodesAcked, onAckTimeout and onFailure methods
        // and since we override those methods here and use future we don't need listener
        super(priority, request, null);
        this.clusterService = clusterService;
        this.future = new CompletableFuture<>();
        this.hintOnError = hintOnError;
        this.task = task;
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        // Implemented since it's obligatory but not used
        // as both consumers of this method (onAckTimeout and onAllNodesAcked) are overwritten.
        return new AcknowledgedResponse(acknowledged);
    }

    @Override
    public void onAckTimeout() {
        future.completeExceptionally(new TimeoutException(hintOnError));
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        // Check if we're still the elected master before sending the task.
        if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
            throw new IllegalStateException("Master was re-elected."); // Hint gets added in onFailure()
        } else {
            return task.apply(currentState);
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        future.completeExceptionally(new IllegalStateException(hintOnError, e));
    }

    @Override
    public void onAllNodesAcked(@Nullable Exception e) {
        if (e == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(new IllegalStateException(hintOnError, e));
        }
    }
}
