/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.FunctionProvider.FunctionFactory;
import io.crate.metadata.functions.Signature;

public abstract class AbstractFunctionModule<T extends FunctionImplementation> extends AbstractModule {

    private HashMap<FunctionName, List<FunctionProvider>> functionImplementations = new HashMap<>();
    private MapBinder<FunctionName, List<FunctionProvider>> implementationsBinder;

    public void register(Signature signature, FunctionFactory factory) {
        List<FunctionProvider> functions = functionImplementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        var duplicate = functions.stream().filter(fr -> fr.signature().equals(signature)).findFirst();
        if (duplicate.isPresent()) {
            throw new IllegalStateException(
                "A function already exists for signature = " + signature);
        }
        functions.add(new FunctionProvider(signature, factory));
    }

    public abstract void configureFunctions();

    @Override
    protected void configure() {
        configureFunctions();

        implementationsBinder = MapBinder.newMapBinder(
            binder(),
            new TypeLiteral<FunctionName>() {},
            new TypeLiteral<List<FunctionProvider>>() {});
        for (Map.Entry<FunctionName, List<FunctionProvider>> entry : functionImplementations.entrySet()) {
            implementationsBinder.addBinding(entry.getKey()).toProvider(entry::getValue);
        }

        // clear registration maps
        functionImplementations = null;
    }
}
