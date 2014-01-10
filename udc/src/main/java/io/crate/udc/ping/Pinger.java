package io.crate.udc.ping;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.node.service.NodeService;

import java.util.TimerTask;

public class Pinger extends TimerTask {

    private ESLogger logger = Loggers.getLogger(this.getClass());

    private final ClusterService clusterService;
    private final NodeService nodeService;

    private long successCounter = 0;
    private long failCounter = 0;

    @Inject
    public Pinger(ClusterService clusterService, NodeService nodeService) {
        this.clusterService = clusterService;
        this.nodeService = nodeService;
    }

    @Override
    public void run() {
        logger.info("ping");
    }
}
