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

import io.crate.expression.AbstractFunctionModule;
import io.crate.expression.scalar.arithmetic.AbsFunction;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.scalar.arithmetic.CeilFunction;
import io.crate.expression.scalar.arithmetic.ExpFunction;
import io.crate.expression.scalar.arithmetic.FloorFunction;
import io.crate.expression.scalar.arithmetic.IntervalArithmeticScalar;
import io.crate.expression.scalar.arithmetic.IntervalTimestampArithmeticScalar;
import io.crate.expression.scalar.arithmetic.LogFunction;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.scalar.arithmetic.NegateFunctions;
import io.crate.expression.scalar.arithmetic.RadiansDegreesFunctions;
import io.crate.expression.scalar.arithmetic.RandomFunction;
import io.crate.expression.scalar.arithmetic.RoundFunction;
import io.crate.expression.scalar.arithmetic.SquareRootFunction;
import io.crate.expression.scalar.arithmetic.TrigonometricFunctions;
import io.crate.expression.scalar.arithmetic.TruncFunction;
import io.crate.expression.scalar.cast.CastFunction;
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
import io.crate.expression.scalar.postgres.CurrentSettingFunction;
import io.crate.expression.scalar.postgres.PgBackendPidFunction;
import io.crate.expression.scalar.postgres.PgGetUserByIdFunction;
import io.crate.expression.scalar.regex.MatchesFunction;
import io.crate.expression.scalar.regex.RegexpReplaceFunction;
import io.crate.expression.scalar.string.AsciiFunction;
import io.crate.expression.scalar.string.EncodeDecodeFunction;
import io.crate.expression.scalar.string.HashFunctions;
import io.crate.expression.scalar.string.InitCapFunction;
import io.crate.expression.scalar.string.LengthFunction;
import io.crate.expression.scalar.string.QuoteIdentFunction;
import io.crate.expression.scalar.string.ReplaceFunction;
import io.crate.expression.scalar.string.StringCaseFunction;
import io.crate.expression.scalar.string.StringLeftRightFunction;
import io.crate.expression.scalar.string.StringPaddingFunction;
import io.crate.expression.scalar.string.StringRepeatFunction;
import io.crate.expression.scalar.string.TrimFunctions;
import io.crate.expression.scalar.systeminformation.CurrentSchemaFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemasFunction;
import io.crate.expression.scalar.systeminformation.ObjDescriptionFunction;
import io.crate.expression.scalar.systeminformation.PgGetExpr;
import io.crate.expression.scalar.systeminformation.PgTypeofFunction;
import io.crate.expression.scalar.systeminformation.VersionFunction;
import io.crate.expression.scalar.timestamp.CurrentTimeFunction;
import io.crate.expression.scalar.timestamp.CurrentTimestampFunction;
import io.crate.expression.scalar.timestamp.NowFunction;
import io.crate.expression.scalar.timestamp.TimezoneFunction;
import io.crate.metadata.FunctionImplementation;

public class ScalarFunctionModule extends AbstractFunctionModule<FunctionImplementation> {

    @Override
    public void configureFunctions() {
        NegateFunctions.register(this);
        CollectionCountFunction.register(this);
        CollectionAverageFunction.register(this);
        FormatFunction.register(this);
        SubstrFunction.register(this);
        MatchesFunction.register(this);
        RegexpReplaceFunction.register(this);

        ArithmeticFunctions.register(this);
        IntervalTimestampArithmeticScalar.register(this);
        IntervalArithmeticScalar.register(this);

        DistanceFunction.register(this);
        WithinFunction.register(this);
        IntersectsFunction.register(this);
        CoordinateFunction.register(this);
        GeoHashFunction.register(this);

        SubscriptFunction.register(this);
        SubscriptObjectFunction.register(this);
        SubscriptRecordFunction.register(this);

        RoundFunction.register(this);
        CeilFunction.register(this);
        RandomFunction.register(this);
        AbsFunction.register(this);
        FloorFunction.register(this);
        TruncFunction.register(this);
        SquareRootFunction.register(this);
        LogFunction.register(this);
        TrigonometricFunctions.register(this);
        PiFunction.register(this);
        RadiansDegreesFunctions.register(this);
        ExpFunction.register(this);

        DateTruncFunction.register(this);
        ExtractFunctions.register(this);
        CurrentTimestampFunction.register(this);
        CurrentTimeFunction.register(this);
        NowFunction.register(this);
        TimezoneFunction.register(this);
        DateFormatFunction.register(this);
        CastFunction.register(this);

        StringCaseFunction.register(this);
        StringLeftRightFunction.register(this);
        StringPaddingFunction.register(this);
        InitCapFunction.register(this);
        TrimFunctions.register(this);
        AsciiFunction.register(this);
        EncodeDecodeFunction.register(this);
        StringRepeatFunction.register(this);

        ConcatFunction.register(this);

        LengthFunction.register(this);
        HashFunctions.register(this);
        ReplaceFunction.register(this);
        QuoteIdentFunction.register(this);

        Ignore3vlFunction.register(this);

        MapFunction.register(this);
        ArrayFunction.register(this);
        ArrayCatFunction.register(this);
        ArrayDifferenceFunction.register(this);
        ArrayUniqueFunction.register(this);
        ArrayUpperFunction.register(this);
        ArrayLowerFunction.register(this);
        StringToArrayFunction.register(this);

        CoalesceFunction.register(this);
        GreatestFunction.register(this);
        LeastFunction.register(this);
        NullIfFunction.register(this);
        IfFunction.register(this);

        CurrentSchemaFunction.register(this);
        CurrentSchemasFunction.register(this);
        PgGetExpr.register(this);
        CurrentSettingFunction.register(this);

        PgBackendPidFunction.register(this);
        PgGetUserByIdFunction.register(this);
        PgTypeofFunction.register(this);
        CurrentDatabaseFunction.register(this);
        VersionFunction.register(this);
        ObjDescriptionFunction.register(this);
    }
}
