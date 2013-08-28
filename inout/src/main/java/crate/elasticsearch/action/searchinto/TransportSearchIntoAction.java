package crate.elasticsearch.action.searchinto;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import crate.elasticsearch.action.searchinto.parser.SearchIntoParser;
import crate.elasticsearch.searchinto.Writer;


/**
 *
 */
public class TransportSearchIntoAction extends AbstractTransportSearchIntoAction {

    @Inject
    public TransportSearchIntoAction(Settings settings,
            ThreadPool threadPool, ClusterService clusterService,
            TransportService transportService, CacheRecycler cacheRecycler,
            IndicesService indicesService, ScriptService scriptService,
            SearchIntoParser parser, Writer writer) {
        super(settings, threadPool, clusterService, transportService, cacheRecycler, indicesService,
            scriptService, parser, writer);
    }

    @Override
    protected String transportAction() {
        return SearchIntoAction.NAME;
    }

}
