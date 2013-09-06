package org.cratedb.module.dump;


import org.cratedb.action.dump.DumpAction;
import org.cratedb.action.dump.TransportDumpAction;
import org.cratedb.action.dump.parser.DumpParser;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class DumpModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportDumpAction.class).asEagerSingleton();

        bind(DumpParser.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);

        transportActionsBinder.addBinding(DumpAction.INSTANCE).to(TransportDumpAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(DumpAction.NAME).toInstance(DumpAction.INSTANCE);
    }
}