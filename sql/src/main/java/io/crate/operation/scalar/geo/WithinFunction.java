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

import com.google.common.collect.ImmutableList;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
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
        for (DataType pointType : Arrays.asList(DataTypes.GEO_POINT, DataTypes.STRING)) {
            for (DataType shapeType : Arrays.asList(DataTypes.GEO_SHAPE, DataTypes.STRING, DataTypes.OBJECT)) {
                scalarFunctionModule.register(new WithinFunction(info(pointType, shapeType)));
            }
        }
    }

    private static FunctionInfo info(DataType pointType, DataType shapeType) {
        return new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(pointType, shapeType)),
                DataTypes.BOOLEAN
        );
    }

    private static final FunctionInfo SHAPE_INFO = info(DataTypes.GEO_POINT, DataTypes.GEO_SHAPE);

    private final FunctionInfo info;

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
            if (!leftType.equals(DataTypes.GEO_POINT)) {
                left = Literal.convert(left, DataTypes.GEO_POINT);
                literalConverted = true;
            }
        }

        if (right.symbolType().isValueSymbol()) {
            numLiterals++;
            if (!rightType.equals(DataTypes.GEO_SHAPE)) {
                right = Literal.convert(right, DataTypes.GEO_SHAPE);
                literalConverted = true;
            }
        }

        if (numLiterals == 2) {
            return Literal.newLiteral(evaluate((Input)left, (Input)right));
        }

        if (literalConverted) {
            return new Function(SHAPE_INFO, Arrays.asList(left, right));
        }

        return symbol;
    }
}
