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
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WithinFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "within";

    private static final List<DataType<?>> LEFT_TYPES = List.of(
        DataTypes.GEO_POINT,
        DataTypes.GEO_SHAPE,
        ObjectType.untyped(),
        DataTypes.STRING,
        DataTypes.UNDEFINED
    );

    private static final List<DataType<?>> RIGHT_TYPES = List.of(
        DataTypes.GEO_SHAPE,
        ObjectType.untyped(),
        DataTypes.STRING,
        DataTypes.UNDEFINED
    );

    public static void register(ScalarFunctionModule module) {
        for (DataType<?> left : LEFT_TYPES) {
            for (DataType<?> right : RIGHT_TYPES) {
                module.register(
                    Signature.scalar(
                        NAME,
                        left.getTypeSignature(),
                        right.getTypeSignature(),
                        DataTypes.BOOLEAN.getTypeSignature()
                    )
                        .withForbiddenCoercion(),
                    (signature, args) -> new WithinFunction(info(left, right), signature)
                );
            }
        }
    }

    private static FunctionInfo info(DataType<?> pointType, DataType<?> shapeType) {
        return new FunctionInfo(
            new FunctionIdent(NAME, List.of(pointType, shapeType)),
            DataTypes.BOOLEAN
        );
    }

    private static final FunctionInfo SHAPE_INFO = info(DataTypes.GEO_POINT, DataTypes.GEO_SHAPE);

    private final FunctionInfo info;
    private final Signature signature;

    private WithinFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length == 2 : "number of args must be 2";
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
        return parseLeftShape(left).relate(parseRightShape(right)) == SpatialRelation.WITHIN;
    }

    @SuppressWarnings("unchecked")
    private static Shape parseLeftShape(Object left) {
        Shape shape;
        if (left instanceof Point) {
            Point point = (Point) left;
            shape = SpatialContext.GEO.getShapeFactory().pointXY(point.getX(), point.getY());
        } else if (left instanceof Double[]) {
            Double[] values = (Double[]) left;
            shape = SpatialContext.GEO.getShapeFactory().pointXY(values[0], values[1]);
        } else if (left instanceof String) {
            shape = GeoJSONUtils.wkt2Shape((String) left);
        } else {
            shape = GeoJSONUtils.map2Shape((Map<String, Object>) left);
        }
        return shape;
    }

    @SuppressWarnings("unchecked")
    private Shape parseRightShape(Object right) {
        return (right instanceof String) ?
            GeoJSONUtils.wkt2Shape((String) right) :
            GeoJSONUtils.map2Shape((Map<String, Object>) right);
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

        boolean literalConverted = false;
        short numLiterals = 0;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
            Symbol converted = left.cast(DataTypes.GEO_POINT);
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
            return Literal.of(evaluate((Input) left, (Input) right));
        }

        if (literalConverted) {
            return new Function(SHAPE_INFO, signature, Arrays.asList(left, right));
        }

        return symbol;
    }
}
