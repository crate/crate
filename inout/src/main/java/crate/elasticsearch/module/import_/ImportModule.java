package crate.elasticsearch.module.import_;

import crate.elasticsearch.action.import_.ImportAction;
import crate.elasticsearch.action.import_.TransportImportAction;
import crate.elasticsearch.action.import_.parser.ImportParser;
import crate.elasticsearch.import_.Importer;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class ImportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportImportAction.class).asEagerSingleton();

        bind(ImportParser.class).asEagerSingleton();
        bind(Importer.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);
        transportActionsBinder.addBinding(ImportAction.INSTANCE).to(TransportImportAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(ImportAction.NAME).toInstance(ImportAction.INSTANCE);

    }

}
