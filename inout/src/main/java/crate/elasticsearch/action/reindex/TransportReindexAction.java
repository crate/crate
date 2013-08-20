package crate.elasticsearch.action.reindex;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import crate.elasticsearch.action.searchinto.AbstractTransportSearchIntoAction;
import crate.elasticsearch.searchinto.Writer;

public class TransportReindexAction extends AbstractTransportSearchIntoAction {

    @Inject
    public TransportReindexAction(Settings settings, ThreadPool threadPool,
            ClusterService clusterService, TransportService transportService, CacheRecycler cacheRecycler,
            IndicesService indicesService, ScriptService scriptService,
            ReindexParser parser, Writer writer) {
        super(settings, threadPool, clusterService, transportService, cacheRecycler, indicesService,
                scriptService, parser, writer);
    }

    @Override
    protected String transportAction() {
        return ReindexAction.NAME;
    }

}
