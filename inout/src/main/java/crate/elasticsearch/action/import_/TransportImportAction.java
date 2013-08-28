package crate.elasticsearch.action.import_;

import crate.elasticsearch.action.import_.parser.ImportParser;
import crate.elasticsearch.import_.Importer;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportImportAction extends AbstractTransportImportAction {

    @Inject
    public TransportImportAction(Settings settings, ClusterName clusterName,
                                         ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ImportParser importParser, Importer importer, NodeEnvironment nodeEnv) {
        super(settings, clusterName, threadPool, clusterService, transportService, importParser, importer, nodeEnv);
    }

    @Override
    protected String transportAction() {
        return ImportAction.NAME;
    }
}
