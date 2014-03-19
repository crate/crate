package io.crate.operation.operator;

import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class OperatorModule extends AbstractModule {

    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;

    public void registerOperatorFunction(FunctionImplementation impl) {
        functionBinder.addBinding(impl.info().ident()).toInstance(impl);
    }

    @Override
    protected void configure() {
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);

        // currently there are no dynamic operator functions, but the binder need to exist
        // so that guice can create the Functions class
        MapBinder.newMapBinder(binder(), String.class, DynamicFunctionResolver.class);

        AndOperator.register(this);
        OrOperator.register(this);
        EqOperator.register(this);
        LtOperator.register(this);
        LteOperator.register(this);
        GtOperator.register(this);
        GteOperator.register(this);
        LikeOperator.register(this);
        InOperator.register(this);
    }
}
