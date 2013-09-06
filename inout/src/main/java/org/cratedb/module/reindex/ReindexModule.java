package org.cratedb.module.reindex;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import org.cratedb.action.reindex.ReindexAction;
import org.cratedb.action.reindex.ReindexParser;
import org.cratedb.action.reindex.TransportReindexAction;

public class ReindexModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportReindexAction.class).asEagerSingleton();

        bind(ReindexParser.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder =
                MapBinder.newMapBinder(
                        binder(), GenericAction.class, TransportAction.class);

        transportActionsBinder.addBinding(ReindexAction.INSTANCE).to(
                TransportReindexAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder
                .newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(ReindexAction.NAME).toInstance(
                ReindexAction.INSTANCE);

    }

}
