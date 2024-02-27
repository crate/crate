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

package io.crate.expression.scalar;

import org.elasticsearch.common.settings.Settings;

import io.crate.expression.scalar.arithmetic.AbsFunction;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.scalar.arithmetic.CeilFunction;
import io.crate.expression.scalar.arithmetic.ExpFunction;
import io.crate.expression.scalar.arithmetic.FloorFunction;
import io.crate.expression.scalar.arithmetic.IntervalArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.IntervalTimestampArithmeticScalar;
import io.crate.expression.scalar.arithmetic.LogFunction;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.scalar.arithmetic.NegateFunctions;
import io.crate.expression.scalar.arithmetic.RadiansDegreesFunctions;
import io.crate.expression.scalar.arithmetic.RandomFunction;
import io.crate.expression.scalar.arithmetic.RoundFunction;
import io.crate.expression.scalar.arithmetic.SquareRootFunction;
import io.crate.expression.scalar.arithmetic.SubtractTimestampScalar;
import io.crate.expression.scalar.arithmetic.TrigonometricFunctions;
import io.crate.expression.scalar.arithmetic.TruncFunction;
import io.crate.expression.scalar.bitwise.BitwiseFunctions;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.scalar.cast.TryCastFunction;
import io.crate.expression.scalar.conditional.CaseFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.scalar.conditional.GreatestFunction;
import io.crate.expression.scalar.conditional.IfFunction;
import io.crate.expression.scalar.conditional.LeastFunction;
import io.crate.expression.scalar.conditional.NullIfFunction;
import io.crate.expression.scalar.formatting.ToCharFunction;
import io.crate.expression.scalar.geo.AreaFunction;
import io.crate.expression.scalar.geo.CoordinateFunction;
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.scalar.geo.GeoHashFunction;
import io.crate.expression.scalar.geo.IntersectsFunction;
import io.crate.expression.scalar.geo.WithinFunction;
import io.crate.expression.scalar.object.ObjectKeysFunction;
import io.crate.expression.scalar.postgres.CurrentSettingFunction;
import io.crate.expression.scalar.postgres.PgBackendPidFunction;
import io.crate.expression.scalar.postgres.PgEncodingToCharFunction;
import io.crate.expression.scalar.postgres.PgGetUserByIdFunction;
import io.crate.expression.scalar.postgres.PgPostmasterStartTime;
import io.crate.expression.scalar.regex.RegexpReplaceFunction;
import io.crate.expression.scalar.string.AsciiFunction;
import io.crate.expression.scalar.string.ChrFunction;
import io.crate.expression.scalar.string.EncodeDecodeFunction;
import io.crate.expression.scalar.string.HashFunctions;
import io.crate.expression.scalar.string.InitCapFunction;
import io.crate.expression.scalar.string.LengthFunction;
import io.crate.expression.scalar.string.ParseURIFunction;
import io.crate.expression.scalar.string.ParseURLFunction;
import io.crate.expression.scalar.string.QuoteIdentFunction;
import io.crate.expression.scalar.string.ReplaceFunction;
import io.crate.expression.scalar.string.StringCaseFunction;
import io.crate.expression.scalar.string.StringLeftRightFunction;
import io.crate.expression.scalar.string.StringPaddingFunction;
import io.crate.expression.scalar.string.StringRepeatFunction;
import io.crate.expression.scalar.string.StringSplitPartFunction;
import io.crate.expression.scalar.string.TranslateFunction;
import io.crate.expression.scalar.string.TrimFunctions;
import io.crate.expression.scalar.systeminformation.ColDescriptionFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemaFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemasFunction;
import io.crate.expression.scalar.systeminformation.FormatTypeFunction;
import io.crate.expression.scalar.systeminformation.ObjDescriptionFunction;
import io.crate.expression.scalar.systeminformation.PgFunctionIsVisibleFunction;
import io.crate.expression.scalar.systeminformation.PgGetExpr;
import io.crate.expression.scalar.systeminformation.PgGetFunctionResultFunction;
import io.crate.expression.scalar.systeminformation.PgGetPartkeydefFunction;
import io.crate.expression.scalar.systeminformation.PgGetSerialSequenceFunction;
import io.crate.expression.scalar.systeminformation.PgTypeofFunction;
import io.crate.expression.scalar.systeminformation.VersionFunction;
import io.crate.expression.scalar.timestamp.CurrentTimeFunction;
import io.crate.expression.scalar.timestamp.CurrentTimestampFunction;
import io.crate.expression.scalar.timestamp.NowFunction;
import io.crate.expression.scalar.timestamp.TimezoneFunction;
import io.crate.metadata.Functions;
import io.crate.metadata.FunctionsProvider;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.role.scalar.UserFunction;

public class ScalarFunctions implements FunctionsProvider {

    public void addFunctions(Settings settings,
                             SessionSettingRegistry sessionSettingRegistry,
                             Functions.Builder builder) {
        UserFunction.register(builder);

        NegateFunctions.register(builder);
        CollectionCountFunction.register(builder);
        CollectionAverageFunction.register(builder);
        FormatFunction.register(builder);
        SubstrFunction.register(builder);
        RegexpReplaceFunction.register(builder);

        ArithmeticFunctions.register(builder);
        BitwiseFunctions.register(builder);
        SubtractTimestampScalar.register(builder);
        IntervalTimestampArithmeticScalar.register(builder);
        IntervalArithmeticFunctions.register(builder);

        DistanceFunction.register(builder);
        WithinFunction.register(builder);
        IntersectsFunction.register(builder);
        CoordinateFunction.register(builder);
        GeoHashFunction.register(builder);
        AreaFunction.register(builder);

        SubscriptFunction.register(builder);
        SubscriptObjectFunction.register(builder);
        SubscriptRecordFunction.register(builder);

        RoundFunction.register(builder);
        CeilFunction.register(builder);
        RandomFunction.register(builder);
        AbsFunction.register(builder);
        FloorFunction.register(builder);
        TruncFunction.register(builder);
        SquareRootFunction.register(builder);
        LogFunction.register(builder);
        TrigonometricFunctions.register(builder);
        PiFunction.register(builder);
        RadiansDegreesFunctions.register(builder);
        ExpFunction.register(builder);

        DateTruncFunction.register(builder);
        ExtractFunctions.register(builder);
        CurrentTimeFunction.register(builder);
        CurrentTimestampFunction.register(builder);
        NowFunction.register(builder);
        TimezoneFunction.register(builder);
        DateFormatFunction.register(builder);
        CurrentDateFunction.register(builder);
        DateBinFunction.register(builder);
        AgeFunction.register(builder);

        ToCharFunction.register(builder);

        ExplicitCastFunction.register(builder);
        ImplicitCastFunction.register(builder);
        TryCastFunction.register(builder);

        StringCaseFunction.register(builder);
        StringLeftRightFunction.register(builder);
        StringPaddingFunction.register(builder);
        InitCapFunction.register(builder);
        TrimFunctions.register(builder);
        AsciiFunction.register(builder);
        EncodeDecodeFunction.register(builder);
        StringRepeatFunction.register(builder);
        StringSplitPartFunction.register(builder);
        ChrFunction.register(builder);
        GenRandomTextUUIDFunction.register(builder);

        TranslateFunction.register(builder);
        ConcatFunction.register(builder);
        ConcatWsFunction.register(builder);

        LengthFunction.register(builder);
        HashFunctions.register(builder);
        ReplaceFunction.register(builder);
        QuoteIdentFunction.register(builder);

        Ignore3vlFunction.register(builder);

        MapFunction.register(builder);
        ArrayFunction.register(builder);
        ArrayAppendFunction.register(builder);
        ArrayCatFunction.register(builder);
        ArrayDifferenceFunction.register(builder);
        ArrayUniqueFunction.register(builder);
        ArrayUpperFunction.register(builder);
        ArrayLowerFunction.register(builder);
        StringToArrayFunction.register(builder);
        ArrayToStringFunction.register(builder);
        ArrayMinFunction.register(builder);
        ArrayMaxFunction.register(builder);
        ArraySumFunction.register(builder);
        ArrayAvgFunction.register(builder);
        ArraySliceFunction.register(builder);
        ArrayPositionFunction.register(builder);
        ArrayUnnestFunction.register(builder);
        ArraySetFunction.register(builder);

        CoalesceFunction.register(builder);
        GreatestFunction.register(builder);
        LeastFunction.register(builder);
        NullIfFunction.register(builder);
        IfFunction.register(builder);
        CaseFunction.register(builder);
        NullOrEmptyFunction.register(builder);

        CurrentSchemaFunction.register(builder);
        CurrentSchemasFunction.register(builder);
        PgGetExpr.register(builder);
        PgGetPartkeydefFunction.register(builder);
        CurrentSettingFunction.register(builder, sessionSettingRegistry);

        PgBackendPidFunction.register(builder);
        PgEncodingToCharFunction.register(builder);
        PgGetUserByIdFunction.register(builder);
        PgTypeofFunction.register(builder);
        CurrentDatabaseFunction.register(builder);
        VersionFunction.register(builder);
        ColDescriptionFunction.register(builder);
        ObjDescriptionFunction.register(builder);
        FormatTypeFunction.register(builder);
        PgFunctionIsVisibleFunction.register(builder);
        PgGetFunctionResultFunction.register(builder);
        PgPostmasterStartTime.register(builder);
        PgGetSerialSequenceFunction.register(builder);

        ObjectKeysFunction.register(builder);

        HasSchemaPrivilegeFunction.register(builder);
        HasDatabasePrivilegeFunction.register(builder);
        ParseURIFunction.register(builder);
        ParseURLFunction.register(builder);

        KnnMatch.register(builder);
    }
}
