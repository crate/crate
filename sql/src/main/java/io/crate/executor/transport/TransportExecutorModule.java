package io.crate.executor.transport;

import org.elasticsearch.common.inject.AbstractModule;

public class TransportExecutorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportCollectNodeAction.class).asEagerSingleton();
    }
}
