package io.crate.metadata;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class MetaDataModule extends AbstractModule {
    @Override
    protected void configure() {

        MapBinder.newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
        bind(ReferenceResolver.class).to(GlobalReferenceResolver.class).asEagerSingleton();

        MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        bind(Functions.class).asEagerSingleton();


    }
}
