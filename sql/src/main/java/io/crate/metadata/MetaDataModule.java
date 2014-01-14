package io.crate.metadata;

import org.elasticsearch.common.inject.AbstractModule;

public class MetaDataModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ReferenceResolver.class).to(GlobalReferenceResolver.class).asEagerSingleton();
    }
}
