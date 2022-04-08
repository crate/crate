
package io.crate.replication.logical;

import java.net.ConnectException;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;

import io.crate.common.unit.TimeValue;

public class LogicalReplicationRetry {

    /**
     * Exponential policy up to roughly 30 minutes. This should be enough to survive
     * publisher unavailability due to a node restart or intermediate failures.
     **/
    public static final BackoffPolicy FAILURE_BACKOFF_POLICY = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), 16);

    public static boolean failureIsTemporary(Throwable t) {
        // - handle only? blocked by: [SERVICE_UNAVAILABLE/1/state not recovered / initialized]; instead of all clusterBlockExceptions
        return t instanceof NodeNotConnectedException
            || t instanceof NodeClosedException
            || t instanceof NodeDisconnectedException
            || t instanceof ConnectTransportException
            || t instanceof ConnectException
            || t instanceof ClusterBlockException
            || t instanceof NoSeedNodeLeftException
            || t instanceof NoShardAvailableActionException
            || t instanceof AlreadyClosedException;
    }
}
