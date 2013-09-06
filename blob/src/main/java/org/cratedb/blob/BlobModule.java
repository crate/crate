package org.cratedb.blob;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class BlobModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(BlobService.class).asEagerSingleton();

        bind(TransportPutChunkAction.class).asEagerSingleton();
        bind(TransportStartBlobAction.class).asEagerSingleton();
        bind(TransportDeleteBlobAction.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class,
                TransportAction.class);
        transportActionsBinder.addBinding(PutChunkAction.INSTANCE).to(TransportPutChunkAction.class).asEagerSingleton();
        transportActionsBinder.addBinding(StartBlobAction.INSTANCE).to(TransportStartBlobAction.class).asEagerSingleton();
        transportActionsBinder.addBinding(DeleteBlobAction.INSTANCE).to(TransportDeleteBlobAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(PutChunkAction.NAME).toInstance(PutChunkAction.INSTANCE);
        actionsBinder.addBinding(StartBlobAction.NAME).toInstance(StartBlobAction.INSTANCE);
        actionsBinder.addBinding(DeleteBlobAction.NAME).toInstance(DeleteBlobAction.INSTANCE);
    }
}
