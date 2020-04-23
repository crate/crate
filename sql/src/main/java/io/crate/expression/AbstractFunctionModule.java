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

package io.crate.expression;

import io.crate.metadata.FuncResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public abstract class AbstractFunctionModule<T extends FunctionImplementation> extends AbstractModule {

    private Map<FunctionIdent, T> functions = new HashMap<>();
    private Map<FunctionName, FunctionResolver> resolver = new HashMap<>();
    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
    private MapBinder<FunctionName, FunctionResolver> resolverBinder;

    private HashMap<FunctionName, List<FuncResolver>> functionImplementations = new HashMap<>();
    private MapBinder<FunctionName, List<FuncResolver>> implementationsBinder;

    /**
     * @deprecated Use {@link #register(Signature, BiFunction)} instead.
     */
    public void register(T impl) {
        functions.put(impl.info().ident(), impl);
    }

    /**
     * @deprecated Use {@link #register(Signature, BiFunction)} instead.
     */
    public void register(String name, FunctionResolver functionResolver) {
        register(new FunctionName(name), functionResolver);
    }

    /**
     * @deprecated Use {@link #register(Signature, BiFunction)} instead.
     */
    public void register(FunctionName qualifiedName, FunctionResolver functionResolver) {
        resolver.put(qualifiedName, functionResolver);
    }

    public void register(Signature signature, BiFunction<Signature, List<DataType>, FunctionImplementation> factory) {
        List<FuncResolver> functions = functionImplementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        var duplicate = functions.stream().filter(fr -> fr.getSignature().equals(signature)).findFirst();
        if (duplicate.isPresent()) {
            throw new IllegalStateException(
                "A function already exists for signature = " + signature);
        }
        functions.add(new FuncResolver(signature, factory));
    }

    public abstract void configureFunctions();

    @Override
    protected void configure() {
        configureFunctions();
        // bind all registered functions and resolver
        // by doing it here instead of the register functions, plugins can also use the
        // register functions in their onModule(...) hooks
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        resolverBinder = MapBinder.newMapBinder(binder(), FunctionName.class, FunctionResolver.class);
        for (Map.Entry<FunctionIdent, T> entry : functions.entrySet()) {
            functionBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }
        for (Map.Entry<FunctionName, FunctionResolver> entry : resolver.entrySet()) {
            resolverBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }

        // clear registration maps
        functions = null;
        resolver = null;

        // New signature registry
        implementationsBinder = MapBinder.newMapBinder(
            binder(),
            new TypeLiteral<FunctionName>() {},
            new TypeLiteral<List<FuncResolver>>() {});
        for (Map.Entry<FunctionName, List<FuncResolver>> entry : functionImplementations.entrySet()) {
            implementationsBinder.addBinding(entry.getKey()).toProvider(entry::getValue);
        }

        // clear registration maps
        functionImplementations = null;
    }
}
