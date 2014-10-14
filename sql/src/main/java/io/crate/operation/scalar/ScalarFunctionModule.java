/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.scalar;

import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.operation.scalar.arithmetic.*;
import io.crate.operation.scalar.cast.*;
import io.crate.operation.scalar.geo.DistanceFunction;
import io.crate.operation.scalar.geo.WithinFunction;
import io.crate.operation.scalar.regex.MatchesFunction;
import io.crate.operation.scalar.regex.ReplaceFunction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class ScalarFunctionModule extends AbstractModule {

    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
    private MapBinder<String, DynamicFunctionResolver> resolverBinder;

    public void register(FunctionImplementation impl) {
        functionBinder.addBinding(impl.info().ident()).toInstance(impl);
    }

    public void register(String name, DynamicFunctionResolver dynamicFunctionResolver) {
        resolverBinder.addBinding(name).toInstance(dynamicFunctionResolver);
    }

    @Override
    protected void configure() {
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        resolverBinder = MapBinder.newMapBinder(binder(), String.class, DynamicFunctionResolver.class);

        CollectionCountFunction.register(this);
        CollectionAverageFunction.register(this);
        DateTruncFunction.register(this);
        DateTruncTimeZoneAwareFunction.register(this);
        FormatFunction.register(this);
        SubstrFunction.register(this);
        MatchesFunction.register(this);
        ReplaceFunction.register(this);

        AddFunction.register(this);
        SubtractFunction.register(this);
        MultiplyFunction.register(this);
        DivideFunction.register(this);
        ModulusFunction.register(this);

        DistanceFunction.register(this);
        WithinFunction.register(this);

        SubscriptFunction.register(this);

        RoundFunction.register(this);
        CeilFunction.register(this);
        RandomFunction.register(this);
        AbsFunction.register(this);
        FloorFunction.register(this);
        SquareRootFunction.register(this);
        LogFunction.register(this);

        // all cast functions must be registered at CastFunctionResolver
        ToStringFunction.register(this);
        ToStringArrayFunction.register(this);
        ToIntFunction.register(this);
        ToIntArrayFunction.register(this);
        ToLongFunction.register(this);
        ToLongArrayFunction.register(this);
        ToDoubleArrayFunction.register(this);
        ToBooleanFunction.register(this);
        ToDoubleFunction.register(this);
        ToFloatFunction.register(this);
        ToByteFunction.register(this);
        ToShortFunction.register(this);
        ToBooleanArrayFunction.register(this);
    }
}
