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

package io.crate.expression.scalar.geo;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.locationtech.spatial4j.shape.Shape;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static io.crate.metadata.functions.Signature.scalar;

public class IntersectsFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "intersects";

    private static FunctionInfo info(DataType<?> type1, DataType<?> type2) {
        return new FunctionInfo(
            new FunctionIdent(NAME, List.of(type1, type2)),
            DataTypes.BOOLEAN
        );
    }

    private static final FunctionInfo SHAPE_INFO = info(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE);

    private static final List<DataType<?>> SUPPORTED_TYPES = List.of(
        DataTypes.STRING,
        ObjectType.untyped(),
        DataTypes.GEO_SHAPE
    );

    public static void register(ScalarFunctionModule module) {
        for (DataType<?> type1 : SUPPORTED_TYPES) {
            for (DataType<?> type2 : SUPPORTED_TYPES) {
                module.register(
                    scalar(
                        NAME,
                        type1.getTypeSignature(),
                        type2.getTypeSignature(),
                        DataTypes.BOOLEAN.getTypeSignature()
                        ),
                    (signature, args) ->
                        new IntersectsFunction(info(type1, type2), signature)
                );
            }
        }
    }

    private final FunctionInfo info;
    private final Signature signature;

    public IntersectsFunction(FunctionInfo functionInfo, Signature signature) {
        this.info = functionInfo;
        this.signature = signature;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<Object>... args) {
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

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);
        int numLiterals = 0;
        boolean literalConverted = false;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
            Symbol converted = left.cast(DataTypes.GEO_SHAPE);
            literalConverted = converted != right;
            left = converted;
        }

        if (right.symbolType().isValueSymbol()) {
            numLiterals++;
            Symbol converted = right.cast(DataTypes.GEO_SHAPE);
            literalConverted = literalConverted || converted != right;
            right = converted;
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate(txnCtx, (Input) left, (Input) right));
        }

        if (literalConverted) {
            return new Function(SHAPE_INFO, signature, Arrays.asList(left, right));
        }

        return symbol;
    }
}
