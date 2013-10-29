package org.cratedb.service;

import org.cratedb.action.TransportDistributedSQLAction;
import org.cratedb.action.TransportSQLReduceHandler;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class SQLService extends AbstractLifecycleComponent<SQLService> {

    private final TransportSQLReduceHandler transportSQLReduceHandler;
    private final TransportDistributedSQLAction transportDistributedSQLAction;

    public static final String CUSTOM_ANALYZER_SETTINGS_PREFIX = "crate.analyzer.custom";

    @Inject
    public SQLService(Settings settings,
                      TransportSQLReduceHandler transportSQLReduceHandler,
                      TransportDistributedSQLAction transportDistributedSQLAction) {
        super(settings);
        this.transportSQLReduceHandler = transportSQLReduceHandler;
        this.transportDistributedSQLAction = transportDistributedSQLAction;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        transportSQLReduceHandler.registerHandler();
        transportDistributedSQLAction.registerHandler();
    }

    @Override
    protected void doStop() throws ElasticSearchException {

    }

    @Override
    protected void doClose() throws ElasticSearchException {

    }
}
