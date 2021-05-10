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

package io.crate.expression.scalar.geo;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.geo.GeoUtils;
import org.locationtech.spatial4j.shape.Point;

import java.util.Arrays;

import static io.crate.metadata.functions.Signature.scalar;

public class DistanceFunction extends Scalar<Double, Point> {

    public static final String NAME = "distance";

    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                NAME,
                DataTypes.GEO_POINT.getTypeSignature(),
                DataTypes.GEO_POINT.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            DistanceFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private DistanceFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Double evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Point>[] args) {
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
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        Symbol arg1 = symbol.arguments().get(0);
        Symbol arg2 = symbol.arguments().get(1);
        DataType<?> arg1Type = arg1.valueType();
        DataType<?> arg2Type = arg2.valueType();

        boolean arg1IsReference = true;
        short numLiterals = 0;

        if (arg1.symbolType().isValueSymbol()) {
            numLiterals++;
            arg1IsReference = false;
        }

        if (arg2.symbolType().isValueSymbol()) {
            numLiterals++;
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate((Input) arg1, (Input) arg2));
        }

        // ensure reference is the first argument.
        if (!arg1IsReference) {
            return new Function(signature, Arrays.asList(arg2, arg1), signature.getReturnType().createType());
        }
        return symbol;
    }
}
