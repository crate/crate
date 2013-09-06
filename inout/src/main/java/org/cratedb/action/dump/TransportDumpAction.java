package org.cratedb.action.dump;

import org.cratedb.action.dump.parser.DumpParser;
import org.cratedb.action.export.AbstractTransportExportAction;
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
public class TransportDumpAction extends AbstractTransportExportAction {

    @Inject
    public TransportDumpAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                               TransportService transportService, IndicesService indicesService,
                               ScriptService scriptService, CacheRecycler cacheRecycler,
                               DumpParser dumpParser, Exporter exporter, NodeEnvironment nodeEnv) {
        super(settings, threadPool, clusterService, transportService, indicesService, scriptService,
            cacheRecycler, dumpParser, exporter, nodeEnv);
    }

    @Override
    protected String transportAction() {
        return DumpAction.NAME;
    }
}
