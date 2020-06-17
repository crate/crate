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
import io.crate.types.DataTypes;
import org.locationtech.spatial4j.shape.Shape;

import java.util.List;

import static io.crate.metadata.functions.Signature.scalar;

public class IntersectsFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "intersects";

    private static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(NAME, List.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE)),
        DataTypes.BOOLEAN
    );


    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                NAME,
                DataTypes.GEO_SHAPE.getTypeSignature(),
                DataTypes.GEO_SHAPE.getTypeSignature(),
                DataTypes.BOOLEAN.getTypeSignature()
            ),
            (signature, args) -> new IntersectsFunction(signature)
        );
    }

    private final Signature signature;

    public IntersectsFunction(Signature signature) {
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
        return INFO;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);
        int numLiterals = 0;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
        }

        if (right.symbolType().isValueSymbol()) {
            numLiterals++;
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate(txnCtx, (Input) left, (Input) right));
        }

        return symbol;
    }
}
