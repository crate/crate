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

package io.crate.expression.scalar;

import io.crate.expression.scalar.arithmetic.AbsFunction;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.scalar.arithmetic.CeilFunction;
import io.crate.expression.scalar.arithmetic.FloorFunction;
import io.crate.expression.scalar.arithmetic.LogFunction;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.scalar.arithmetic.NegateFunctions;
import io.crate.expression.scalar.arithmetic.RandomFunction;
import io.crate.expression.scalar.arithmetic.RoundFunction;
import io.crate.expression.scalar.arithmetic.SquareRootFunction;
import io.crate.expression.scalar.arithmetic.TrigonometricFunctions;
import io.crate.expression.scalar.cast.CastFunction;
import io.crate.expression.scalar.cast.TryCastScalarFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.scalar.conditional.GreatestFunction;
import io.crate.expression.scalar.conditional.IfFunction;
import io.crate.expression.scalar.conditional.LeastFunction;
import io.crate.expression.scalar.conditional.NullIfFunction;
import io.crate.expression.scalar.geo.CoordinateFunction;
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.scalar.geo.GeoHashFunction;
import io.crate.expression.scalar.geo.IntersectsFunction;
import io.crate.expression.scalar.geo.WithinFunction;
import io.crate.expression.scalar.postgres.PgBackendPidFunction;
import io.crate.expression.scalar.regex.MatchesFunction;
import io.crate.expression.scalar.regex.RegexpReplaceFunction;
import io.crate.expression.scalar.string.HashFunctions;
import io.crate.expression.scalar.string.InitCapFunction;
import io.crate.expression.scalar.string.LengthFunction;
import io.crate.expression.scalar.string.StringCaseFunction;
import io.crate.expression.scalar.string.ReplaceFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemaFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemasFunction;
import io.crate.expression.scalar.systeminformation.PgGetExpr;
import io.crate.expression.scalar.timestamp.CurrentTimestampFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionResolver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

public class ScalarFunctionModule extends AbstractModule {

    private Map<FunctionIdent, FunctionImplementation> functions = new HashMap<>();
    private Map<FunctionName, FunctionResolver> resolver = new HashMap<>();
    private MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
    private MapBinder<FunctionName, FunctionResolver> resolverBinder;

    public void register(FunctionImplementation impl) {
        functions.put(impl.info().ident(), impl);
    }

    public void register(String name, FunctionResolver functionResolver) {
        register(new FunctionName(name), functionResolver);
    }

    public void register(FunctionName qualifiedName, FunctionResolver functionResolver) {
        resolver.put(qualifiedName, functionResolver);
    }

    @Override
    protected void configure() {
        NegateFunctions.register(this);
        CollectionCountFunction.register(this);
        CollectionAverageFunction.register(this);
        FormatFunction.register(this);
        SubstrFunction.register(this);
        MatchesFunction.register(this);
        RegexpReplaceFunction.register(this);

        ArithmeticFunctions.register(this);

        DistanceFunction.register(this);
        WithinFunction.register(this);
        IntersectsFunction.register(this);
        CoordinateFunction.register(this);
        GeoHashFunction.register(this);

        SubscriptFunction.register(this);
        SubscriptObjectFunction.register(this);

        RoundFunction.register(this);
        CeilFunction.register(this);
        RandomFunction.register(this);
        AbsFunction.register(this);
        FloorFunction.register(this);
        SquareRootFunction.register(this);
        LogFunction.register(this);
        TrigonometricFunctions.register(this);

        DateTruncFunction.register(this);
        ExtractFunctions.register(this);
        CurrentTimestampFunction.register(this);
        DateFormatFunction.register(this);
        CastFunction.register(this);
        TryCastScalarFunction.register(this);

        StringCaseFunction.register(this);
        InitCapFunction.register(this);

        ConcatFunction.register(this);

        LengthFunction.register(this);
        HashFunctions.register(this);
        ReplaceFunction.register(this);

        Ignore3vlFunction.register(this);

        MapFunction.register(this);
        ArrayFunction.register(this);
        ArrayCatFunction.register(this);
        ArrayDifferenceFunction.register(this);
        ArrayUniqueFunction.register(this);
        ArrayUpperFunction.register(this);
        ArrayLowerFunction.register(this);

        CoalesceFunction.register(this);
        GreatestFunction.register(this);
        LeastFunction.register(this);
        NullIfFunction.register(this);
        IfFunction.register(this);

        CurrentSchemaFunction.register(this);
        CurrentSchemasFunction.register(this);
        PgGetExpr.register(this);

        PgBackendPidFunction.register(this);
        register(new CurrentDatabaseFunction());

        // bind all registered functions and resolver
        // by doing it here instead of the register functions, plugins can also use the
        // register functions in their onModule(...) hooks
        functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        resolverBinder = MapBinder.newMapBinder(binder(), FunctionName.class, FunctionResolver.class);
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functions.entrySet()) {
            functionBinder.addBinding(entry.getKey()).toInstance(entry.getValue());

        }
        for (Map.Entry<FunctionName, FunctionResolver> entry : resolver.entrySet()) {
            resolverBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }

        // clear registration maps
        functions = null;
        resolver = null;
    }

}
