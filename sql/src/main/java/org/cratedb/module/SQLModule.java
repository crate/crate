package org.cratedb.module;

import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.action.TransportSQLReduceHandler;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.TransportSQLAction;
import org.cratedb.action.sql.analyzer.ClusterUpdateCrateSettingsAction;
import org.cratedb.action.sql.analyzer.TransportClusterUpdateCrateSettingsAction;
import org.cratedb.service.InformationSchemaService;
import org.cratedb.service.SQLParseService;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SQLModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SQLParseService.class).asEagerSingleton();
        bind(TransportSQLAction.class).asEagerSingleton();
        bind(InformationSchemaService.class).asEagerSingleton();
        bind(TransportClusterUpdateCrateSettingsAction.class).asEagerSingleton();
        bind(NodeExecutionContext.class).asEagerSingleton();
        bind(AnalyzerService.class).asEagerSingleton();
        bind(TransportSQLReduceHandler.class).asEagerSingleton();
        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class,
                TransportAction.class);

        MapBinder<String, AggFunction> aggFunctionBinder =
            MapBinder.newMapBinder(binder(), String.class, AggFunction.class);
        aggFunctionBinder.addBinding(CountAggFunction.NAME).to(CountAggFunction.class).asEagerSingleton();

        transportActionsBinder.addBinding(SQLAction.INSTANCE).to(TransportSQLAction.class).asEagerSingleton();
        transportActionsBinder.addBinding(ClusterUpdateCrateSettingsAction.INSTANCE).to(TransportClusterUpdateCrateSettingsAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(SQLAction.NAME).toInstance(SQLAction.INSTANCE);
        actionsBinder.addBinding(ClusterUpdateCrateSettingsAction.NAME).toInstance(ClusterUpdateCrateSettingsAction.INSTANCE);

    }
}