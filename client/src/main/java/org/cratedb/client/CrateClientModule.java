package org.cratedb.client;

import org.elasticsearch.common.inject.AbstractModule;

public class CrateClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(InternalCrateClient.class).asEagerSingleton();
    }
}
