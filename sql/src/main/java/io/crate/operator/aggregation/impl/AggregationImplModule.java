package io.crate.operator.aggregation.impl;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.operator.aggregation.AggregationFunction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class AggregationImplModule extends AbstractModule {

    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;

    public void registerAggregateFunction(AggregationFunction impl) {
        functionBinder.addBinding(impl.info().ident()).toInstance(impl);
    }

    @Override
    protected void configure() {
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        AverageAggregation.register(this);
    }
}
