package io.crate.operation.operator;

import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.operation.operator.any.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class OperatorModule extends AbstractModule {

    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
    private MapBinder<String, DynamicFunctionResolver> dynamicFunctionBinder;

    public void registerOperatorFunction(FunctionImplementation impl) {
        functionBinder.addBinding(impl.info().ident()).toInstance(impl);
    }

    public void registerDynamicOperatorFunction(String name, DynamicFunctionResolver resolver) {
        dynamicFunctionBinder.addBinding(name).toInstance(resolver);
    }

    @Override
    protected void configure() {
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        dynamicFunctionBinder = MapBinder.newMapBinder(binder(), String.class, DynamicFunctionResolver.class);

        AndOperator.register(this);
        OrOperator.register(this);
        EqOperator.register(this);
        LtOperator.register(this);
        LteOperator.register(this);
        GtOperator.register(this);
        GteOperator.register(this);
        LikeOperator.register(this);
        InOperator.register(this);

        AnyEqOperator.register(this);
        AnyNeqOperator.register(this);
        AnyGteOperator.register(this);
        AnyGtOperator.register(this);
        AnyLteOperator.register(this);
        AnyLtOperator.register(this);
    }
}
