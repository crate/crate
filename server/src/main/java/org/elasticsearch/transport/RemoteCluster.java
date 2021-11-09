package org.elasticsearch.transport;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Transport.Connection;

public class RemoteCluster implements Closeable {

    private final Settings settings;

    public RemoteCluster(Settings settings) {
        this.settings = settings;
    }

    public CompletableFuture<Connection> getConnection(@Nullable DiscoveryNode preferredTargetNode) {
        return null;
    }

    @Override
    public void close() throws IOException {
    }

    // static ConnectionProfile buildConnectionProfile(Settings nodeSettings,
    //                                                 Settings connectionSettings) {
    //     ConnectionStrategy mode = REMOTE_CONNECTION_MODE.get(connectionSettings);
    //     boolean compress = REMOTE_CONNECTION_COMPRESS.exists(connectionSettings)
    //         ? REMOTE_CONNECTION_COMPRESS.get(connectionSettings)
    //         : TransportSettings.TRANSPORT_COMPRESS.get(nodeSettings);
    //     TimeValue pingInterval = REMOTE_CONNECTION_PING_SCHEDULE.exists(connectionSettings)
    //         ? REMOTE_CONNECTION_PING_SCHEDULE.get(connectionSettings)
    //         : TransportSettings.PING_SCHEDULE.get(nodeSettings);
    //     ConnectionProfile.Builder builder = new ConnectionProfile.Builder()
    //         .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(nodeSettings))
    //         .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(nodeSettings))
    //         .setCompressionEnabled(compress)
    //         .setPingInterval(pingInterval)
    //         .addConnections(0, TransportRequestOptions.Type.BULK, TransportRequestOptions.Type.STATE,
    //                         TransportRequestOptions.Type.RECOVERY, TransportRequestOptions.Type.PING)
    //         .addConnections(mode.numberOfChannels, TransportRequestOptions.Type.REG);
    //     return builder.build();
    // }
}

