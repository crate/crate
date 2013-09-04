package crate.elasticsearch.blob.stats;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class BlobStatsModule extends AbstractModule {


    @Override
    protected void configure() {
        bind(TransportBlobStatsAction.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder =
            MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);

        transportActionsBinder
            .addBinding(BlobStatsAction.INSTANCE).to(TransportBlobStatsAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder =
            MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(BlobStatsAction.NAME).toInstance(BlobStatsAction.INSTANCE);
    }
}
