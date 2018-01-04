/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.operator;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionResolver;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.operation.operator.any.AnyGtOperator;
import io.crate.operation.operator.any.AnyGteOperator;
import io.crate.operation.operator.any.AnyLikeOperator;
import io.crate.operation.operator.any.AnyLtOperator;
import io.crate.operation.operator.any.AnyLteOperator;
import io.crate.operation.operator.any.AnyNeqOperator;
import io.crate.operation.operator.any.AnyNotLikeOperator;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

public class OperatorModule extends AbstractModule {

    private Map<FunctionIdent, FunctionImplementation> functions = new HashMap<>();
    private Map<String, FunctionResolver> dynamicFunctionResolvers = new HashMap<>();
    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
    private MapBinder<String, FunctionResolver> dynamicFunctionBinder;

    public void registerOperatorFunction(FunctionImplementation impl) {
        functions.put(impl.info().ident(), impl);
    }

    public void registerDynamicOperatorFunction(String name, FunctionResolver resolver) {
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
        dynamicFunctionBinder = MapBinder.newMapBinder(binder(), String.class, FunctionResolver.class);
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functions.entrySet()) {
            functionBinder.addBinding(entry.getKey()).toInstance(entry.getValue());

        }
        for (Map.Entry<String, FunctionResolver> entry : dynamicFunctionResolvers.entrySet()) {
            dynamicFunctionBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }

        // clear registration maps
        functions = null;
        dynamicFunctionResolvers = null;
    }
}
