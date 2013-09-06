package org.cratedb.action.export;

import org.cratedb.action.export.parser.ExportParser;
import org.cratedb.export.Exporter;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;


/**
 *
 */
public class TransportExportAction extends AbstractTransportExportAction {

    @Inject
    public TransportExportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                 TransportService transportService, IndicesService indicesService,
                                 ScriptService scriptService, CacheRecycler cacheRecycler,
                                 ExportParser exportParser, Exporter exporter, NodeEnvironment nodeEnv) {
        super(settings, threadPool, clusterService, transportService, indicesService, scriptService,
            cacheRecycler, exportParser, exporter, nodeEnv);
    }

    @Override
    protected String transportAction() {
        return ExportAction.NAME;
    }
}
