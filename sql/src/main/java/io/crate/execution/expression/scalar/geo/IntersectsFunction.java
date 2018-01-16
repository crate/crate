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

package io.crate.execution.expression.scalar.geo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.execution.expression.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.locationtech.spatial4j.shape.Shape;

import java.util.Arrays;
import java.util.Set;

public class IntersectsFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "intersects";

    private static FunctionInfo info(DataType type1, DataType type2) {
        return new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.of(type1, type2)),
            DataTypes.BOOLEAN
        );
    }

    private static final FunctionInfo SHAPE_INFO = info(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE);

    private static final Set<DataType> SUPPORTED_TYPES = ImmutableSet.<DataType>of(
        DataTypes.STRING,
        DataTypes.OBJECT,
        DataTypes.GEO_SHAPE
    );

    public static void register(ScalarFunctionModule module) {
        for (DataType type1 : SUPPORTED_TYPES) {
            for (DataType type2 : SUPPORTED_TYPES) {
                module.register(new IntersectsFunction(info(type1, type2)));
            }
        }
    }

    private final FunctionInfo info;

    public IntersectsFunction(FunctionInfo functionInfo) {
        this.info = functionInfo;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert args.length == 2 : "Invalid number of Arguments";
        Object left = args[0].value();
        if (left == null) {
            return null;
        }
        Object right = args[1].value();
        if (right == null) {
            return null;
        }
        Shape leftShape = GeoJSONUtils.map2Shape(DataTypes.GEO_SHAPE.value(left));
        Shape rightShape = GeoJSONUtils.map2Shape(DataTypes.GEO_SHAPE.value(right));
        return leftShape.relate(rightShape).intersects();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);
        int numLiterals = 0;
        boolean literalConverted = false;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
            Symbol converted = convertTo(DataTypes.GEO_SHAPE, (Literal) left);
            literalConverted = converted != right;
            left = converted;
        }

        if (right.symbolType().isValueSymbol()) {
            numLiterals++;
            Symbol converted = convertTo(DataTypes.GEO_SHAPE, (Literal) right);
            literalConverted = literalConverted || converted != right;
            right = converted;
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate((Input) left, (Input) right));
        }

        if (literalConverted) {
            return new Function(SHAPE_INFO, Arrays.asList(left, right));
        }

        return symbol;
    }

    private static Symbol convertTo(DataType toType, Literal convertMe) {
        if (convertMe.valueType().equals(toType)) {
            return convertMe;
        }
        return Literal.convert(convertMe, toType);
    }
}
