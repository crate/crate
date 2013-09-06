package org.cratedb.module.searchinto;

import org.cratedb.action.searchinto.SearchIntoAction;
import org.cratedb.action.searchinto.TransportSearchIntoAction;
import org.cratedb.action.searchinto.parser.SearchIntoParser;
import org.cratedb.searchinto.BulkWriterCollector;
import org.cratedb.searchinto.WriterCollectorFactory;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SearchIntoModule extends AbstractModule {


    @Override
    protected void configure() {
        bind(TransportSearchIntoAction.class).asEagerSingleton();

        bind(SearchIntoParser.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder =
                MapBinder.newMapBinder(
                        binder(), GenericAction.class, TransportAction.class);

        transportActionsBinder.addBinding(SearchIntoAction.INSTANCE).to(
                TransportSearchIntoAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder
                .newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(SearchIntoAction.NAME).toInstance(
                SearchIntoAction.INSTANCE);


        MapBinder<String, WriterCollectorFactory> collectorBinder
                = MapBinder.newMapBinder(binder(),
                String.class, WriterCollectorFactory.class);

        collectorBinder.addBinding(BulkWriterCollector.NAME).toProvider(
                FactoryProvider
                        .newFactory(WriterCollectorFactory.class,
                                BulkWriterCollector.class));


    }
}