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
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.data.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WithinFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "within";

    private static final Set<DataType> LEFT_TYPES = ImmutableSet.<DataType>of(
        DataTypes.GEO_POINT,
        DataTypes.GEO_SHAPE,
        DataTypes.OBJECT,
        DataTypes.STRING
    );

    private static final Set<DataType> RIGHT_TYPES = ImmutableSet.<DataType>of(
        DataTypes.GEO_SHAPE,
        DataTypes.OBJECT,
        DataTypes.STRING
    );

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        for (DataType left : LEFT_TYPES) {
            for (DataType right : RIGHT_TYPES) {
                scalarFunctionModule.register(new WithinFunction(info(left, right)));
            }
        }
    }

    private static FunctionInfo info(DataType pointType, DataType shapeType) {
        return new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.of(pointType, shapeType)),
            DataTypes.BOOLEAN
        );
    }

    private static final FunctionInfo SHAPE_INFO = info(DataTypes.GEO_POINT, DataTypes.GEO_SHAPE);

    private final FunctionInfo info;

    private WithinFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Boolean evaluate(Input[] args) {
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
    private Shape parseLeftShape(Object left) {
        Shape shape;
        if (left instanceof Double[]) {
            Double[] values = (Double[]) left;
            shape = SpatialContext.GEO.makePoint(values[0], values[1]);
        } else if (left instanceof BytesRef) {
            shape = GeoJSONUtils.wkt2Shape(BytesRefs.toString(left));
        } else {
            shape = GeoJSONUtils.map2Shape((Map<String, Object>) left);
        }
        return shape;
    }

    @SuppressWarnings("unchecked")
    private Shape parseRightShape(Object right) {
        return (right instanceof BytesRef) ?
            GeoJSONUtils.wkt2Shape(BytesRefs.toString(right)) :
            GeoJSONUtils.map2Shape((Map<String, Object>) right);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);

        boolean literalConverted = false;
        short numLiterals = 0;

        if (left.symbolType().isValueSymbol()) {
            numLiterals++;
            Symbol converted = convertTo(DataTypes.GEO_POINT, (Literal) left);
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
