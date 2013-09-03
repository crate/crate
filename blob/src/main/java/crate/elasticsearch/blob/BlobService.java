package crate.elasticsearch.blob;

import crate.elasticsearch.blob.exceptions.MissingHTTPEndpointException;
import crate.elasticsearch.blob.pending_transfer.BlobHeadRequestHandler;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.BlobRecoverySource;

import java.util.HashSet;
import java.util.Set;

public class BlobService extends AbstractLifecycleComponent<BlobService> {


    private final Injector injector;
    private final IndicesService indicesService;
    private final BlobHeadRequestHandler blobHeadRequestHandler;

    @Inject
    private BlobRecoverySource blobRecoverySource;


    private final ClusterService clusterService;

    @Inject
    public BlobService(Settings settings,
            ClusterService clusterService, Injector injector,
            IndicesService indicesService, BlobHeadRequestHandler blobHeadRequestHandler) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.injector = injector;
        this.blobHeadRequestHandler = blobHeadRequestHandler;
    }

    public RemoteDigestBlob newBlob(String index, String digest) {
        return new RemoteDigestBlob(this, index, digest);
    }

    public Injector getInjector() {
        return injector;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        logger.info("BlobService.doStart() {}", this);
        blobRecoverySource.registerHandler();
        blobHeadRequestHandler.registerHandler();

        // by default the http server is started after the discovery service.
        // For the BlobService this is too late.

        // The HttpServer has to be started before so that the boundAddress
        // can be added to DiscoveryNodes - this is required for the redirect logic.
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).start();
        } else {
            logger.warn("Http server should be enabled for blob support");
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        //TODO: implement
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        //TODO: implement
    }

    /**
     * @param index  the name of blob-enabled index
     * @param digest sha-1 hash value of the file
     * @return null if no redirect is required, Otherwise the address to which should be redirected.
     */
    public String getRedirectAddress(String index, String digest) throws MissingHTTPEndpointException {
        ShardIterator shards = clusterService.operationRouting().getShards(
                clusterService.state(), index, null, null, digest, "_local");

        ShardRouting shard;
        Set<String> nodeIds = new HashSet<String>();

        // check if one of the shards is on the current node;
        while ((shard = shards.nextOrNull()) != null) {
            if (!shard.active()) {
                continue;
            }
            if (shard.currentNodeId().equals(clusterService.localNode().getId())) {
                return null;
            }
            nodeIds.add(shard.currentNodeId());
        }

        DiscoveryNode node;
        DiscoveryNodes nodes = clusterService.state().getNodes();
        for (String nodeId : nodeIds) {
            node = nodes.get(nodeId);
            if (node.getAttributes().containsKey("http_address")) {
                return node.getAttributes().get("http_address") + "/" + index + "/_blobs/" + digest;
            }
            // else:
            // No HttpServer on node,
            // okay if there are replica nodes with httpServer available
        }

        throw new MissingHTTPEndpointException("Can't find a suitable http server to serve the blob");
    }
}
