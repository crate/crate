package org.cratedb.module;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.TransportSQLAction;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SQLModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportSQLAction.class).asEagerSingleton();
        bind(NodeExecutionContext.class).asEagerSingleton();
        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class,
                TransportAction.class);

        transportActionsBinder.addBinding(SQLAction.INSTANCE).to(TransportSQLAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(SQLAction.NAME).toInstance(SQLAction.INSTANCE);
    }
}