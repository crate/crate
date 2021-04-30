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
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.util.List;
import java.util.Map;

public class WithinFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "within";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.GEO_SHAPE.getTypeSignature(),
                DataTypes.GEO_SHAPE.getTypeSignature(),
                DataTypes.BOOLEAN.getTypeSignature()
            ),
            WithinFunction::new
        );
        // Needed to avoid casts on references of `geo_point` and thus to avoid generic function filter on lucene.
        // Coercion must be forbidden, as string representation could be a `geo_shape` and thus must match
        // the other signature
        for (var type : List.of(DataTypes.GEO_SHAPE, DataTypes.STRING, DataTypes.UNTYPED_OBJECT, DataTypes.UNDEFINED)) {
            module.register(
                Signature.scalar(
                    NAME,
                    DataTypes.GEO_POINT.getTypeSignature(),
                    type.getTypeSignature(),
                    DataTypes.BOOLEAN.getTypeSignature()
                ).withForbiddenCoercion(),
                WithinFunction::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    private WithinFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
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
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);

        short numLiterals = 0;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
        }

        if (right.symbolType().isValueSymbol()) {
            numLiterals++;
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate((Input) left, (Input) right));
        }

        return symbol;
    }
}
