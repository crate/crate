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

package io.crate.operation.scalar.geo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WithinFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "within";

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(NAME, new Resolver());
    }

    private final FunctionInfo info;
    private final static FunctionInfo geoShapeInfo = new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.GEO_POINT, DataTypes.GEO_SHAPE)),
            DataTypes.BOOLEAN
    );

    WithinFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Boolean evaluate(Input[] args) {
        assert args.length == 2;
        return evaluate(args[0], args[1]);
    }

    public Boolean evaluate(Input leftInput, Input rightInput) {
        Object left = leftInput.value();
        if (left == null) {
            return null;
        }
        Object right = rightInput.value();
        if (right == null) {
            return null;
        }

        Shape leftShape;
        if (left instanceof Double[]) {
            Double[] values = (Double[]) left;
            leftShape = SpatialContext.GEO.makePoint(values[0], values[1]);
        } else if (left instanceof List) { // ESSearchTask / ESGetTask returns it as list
            List values = (List) left;
            assert values.size() == 2;
            leftShape = SpatialContext.GEO.makePoint((Double) values.get(0), (Double) values.get(1));
        } else {
            leftShape = GeoJSONUtils.map2Shape((Map<String, Object>)left);
        }
        return leftShape.relate(GeoJSONUtils.map2Shape((Map<String, Object>)right)) == SpatialRelation.WITHIN;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);
        DataType leftType = left.valueType();
        DataType rightType = right.valueType();

        boolean literalConverted = false;
        short numLiterals = 0;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
            if (leftType.equals(DataTypes.STRING)) {
                left = Literal.convert(left, DataTypes.GEO_SHAPE);
                literalConverted = true;
            }
        } else {
            ensureShapeOrPoint(leftType);
        }

        if (right.symbolType().isValueSymbol()) {
            numLiterals++;
            if (rightType.equals(DataTypes.STRING)) {
                right = Literal.convert(right, DataTypes.GEO_SHAPE);
                literalConverted = true;
            }
        } else {
            ensureShapeOrPoint(rightType);
        }

        if (numLiterals == 2) {
            return Literal.newLiteral(evaluate((Input)left, (Input)right));
        }

        if (literalConverted) {
            return new Function(geoShapeInfo, Arrays.asList(left, right));
        }

        return symbol;
    }

    private void ensureShapeOrPoint(DataType symbolType) {
        if (!(symbolType.equals(DataTypes.GEO_POINT) || symbolType.equals(DataTypes.GEO_SHAPE))) {
            throw new IllegalArgumentException(String.format(
                    "\"%s\" doesn't support references of type \"%s\"", NAME, symbolType));
        }
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 2);
            DataType firstType = dataTypes.get(0);

            if (!(firstType.equals(DataTypes.GEO_POINT) ||
                    firstType.equals(DataTypes.GEO_SHAPE) ||
                    firstType.equals(DataTypes.STRING))) {
                throw new IllegalArgumentException(String.format(
                        "%s doesn't take an argument of type \"%s\" as first argument", NAME, firstType));
            }

            DataType secondType = dataTypes.get(1);
            if (!(secondType.equals(DataTypes.GEO_SHAPE) ||
                    secondType.equals(DataTypes.STRING))) {
                throw new IllegalArgumentException(String.format(
                        "%s doesn't take an argument of type \"%s\" as second argument", NAME, secondType));
            }
            return new WithinFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.BOOLEAN));
        }
    }
}
