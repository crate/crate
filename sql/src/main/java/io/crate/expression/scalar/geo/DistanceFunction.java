/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar.geo;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.geo.GeoUtils;
import org.locationtech.spatial4j.shape.Point;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static io.crate.metadata.functions.Signature.scalar;

public class DistanceFunction extends Scalar<Double, Point> {

    public static final String NAME = "distance";
    private static final List<DataType<?>> SUPPORTED_TYPES = List.of(
        DataTypes.STRING, DataTypes.GEO_POINT, DataTypes.DOUBLE_ARRAY);

    private static final FunctionInfo GEO_POINT_INFO = genInfo(Arrays.asList(DataTypes.GEO_POINT, DataTypes.GEO_POINT));

    public static void register(ScalarFunctionModule module) {
        for (var inputType : SUPPORTED_TYPES) {
            module.register(
                scalar(
                    NAME,
                    inputType.getTypeSignature(),
                    inputType.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ),
                (signature, argumentTypes) -> new DistanceFunction(genInfo(argumentTypes), signature)
            );
        }
    }

    private static FunctionInfo genInfo(List<DataType> argumentTypes) {
        return new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.DOUBLE);
    }

    private final FunctionInfo info;
    private final Signature signature;

    private DistanceFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Double evaluate(TransactionContext txnCtx, Input<Point>[] args) {
        assert args.length == 2 : "number of args must be 2";
        return evaluate(args[0], args[1]);
    }

    public static Double evaluate(Input<Point> arg1, Input<Point> arg2) {
        Point value1 = arg1.value();
        if (value1 == null) {
            return null;
        }
        Point value2 = arg2.value();
        if (value2 == null) {
            return null;
        }
        return GeoUtils.arcDistance(value1.getY(), value1.getX(), value2.getY(), value2.getX());
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        Symbol arg1 = symbol.arguments().get(0);
        Symbol arg2 = symbol.arguments().get(1);
        DataType arg1Type = arg1.valueType();
        DataType arg2Type = arg2.valueType();

        boolean arg1IsReference = true;
        boolean literalConverted = false;
        short numLiterals = 0;

        if (arg1.symbolType().isValueSymbol()) {
            numLiterals++;
            arg1IsReference = false;
            if (!arg1Type.equals(DataTypes.GEO_POINT)) {
                literalConverted = true;
                arg1 = arg1.cast(DataTypes.GEO_POINT);
            }
        } else {
            ensureGeoPoint(arg1, arg1Type);
        }

        if (arg2.symbolType().isValueSymbol()) {
            numLiterals++;
            if (!arg2Type.equals(DataTypes.GEO_POINT)) {
                literalConverted = true;
                arg2 = arg2.cast(DataTypes.GEO_POINT);
            }
        } else {
            ensureGeoPoint(arg2, arg2Type);
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate((Input) arg1, (Input) arg2));
        }

        // ensure reference is the first argument.
        if (!arg1IsReference) {
            return new Function(GEO_POINT_INFO, signature, Arrays.asList(arg2, arg1));
        }
        if (literalConverted) {
            return new Function(GEO_POINT_INFO, signature, Arrays.asList(arg1, arg2));
        }
        return symbol;
    }

    private static void ensureGeoPoint(Symbol symbol, DataType dataType) {
        if (!dataType.equals(DataTypes.GEO_POINT)) {
            throw new IllegalArgumentException(Symbols.format(
                "Cannot convert %s to a geo point", symbol));
        }
    }
}
