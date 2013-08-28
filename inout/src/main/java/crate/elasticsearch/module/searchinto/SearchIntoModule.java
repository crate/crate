package crate.elasticsearch.module.searchinto;

import crate.elasticsearch.action.searchinto.SearchIntoAction;
import crate.elasticsearch.action.searchinto.TransportSearchIntoAction;
import crate.elasticsearch.action.searchinto.parser.SearchIntoParser;
import crate.elasticsearch.searchinto.BulkWriterCollector;
import crate.elasticsearch.searchinto.WriterCollectorFactory;
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