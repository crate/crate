package io.crate.operation.operator;

import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.operation.operator.any.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

public class OperatorModule extends AbstractModule {

    private Map<FunctionIdent, FunctionImplementation> functions = new HashMap<>();
    private Map<String, DynamicFunctionResolver> dynamicFunctionResolvers = new HashMap<>();
    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
    private MapBinder<String, DynamicFunctionResolver> dynamicFunctionBinder;

    public void registerOperatorFunction(FunctionImplementation impl) {
        functions.put(impl.info().ident(), impl);
    }

    public void registerDynamicOperatorFunction(String name, DynamicFunctionResolver resolver) {
        dynamicFunctionResolvers.put(name, resolver);
    }

    @Override
    protected void configure() {
        AndOperator.register(this);
        OrOperator.register(this);
        EqOperator.register(this);
        LtOperator.register(this);
        LteOperator.register(this);
        GtOperator.register(this);
        GteOperator.register(this);
        LikeOperator.register(this);
        InOperator.register(this);
        RegexpMatchOperator.register(this);
        RegexpMatchCaseInsensitiveOperator.register(this);

        AnyEqOperator.register(this);
        AnyNeqOperator.register(this);
        AnyGteOperator.register(this);
        AnyGtOperator.register(this);
        AnyLteOperator.register(this);
        AnyLtOperator.register(this);
        AnyLikeOperator.register(this);
        AnyNotLikeOperator.register(this);

        // bind all registered functions and resolver
        // by doing it here instead of the register functions, plugins can also use the
        // register functions in their onModule(...) hooks
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        dynamicFunctionBinder = MapBinder.newMapBinder(binder(), String.class, DynamicFunctionResolver.class);
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functions.entrySet()) {
            functionBinder.addBinding(entry.getKey()).toInstance(entry.getValue());

        }
        for (Map.Entry<String, DynamicFunctionResolver> entry : dynamicFunctionResolvers.entrySet()) {
            dynamicFunctionBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }

        // clear registration maps
        functions = null;
        dynamicFunctionResolvers = null;
    }
}
