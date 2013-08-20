package crate.elasticsearch.module.restore;

import crate.elasticsearch.action.restore.RestoreAction;
import crate.elasticsearch.action.restore.TransportRestoreAction;
import crate.elasticsearch.action.restore.parser.RestoreParser;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class RestoreModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportRestoreAction.class).asEagerSingleton();

        bind(RestoreParser.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);
        transportActionsBinder.addBinding(RestoreAction.INSTANCE).to(TransportRestoreAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(RestoreAction.NAME).toInstance(RestoreAction.INSTANCE);

    }

}
