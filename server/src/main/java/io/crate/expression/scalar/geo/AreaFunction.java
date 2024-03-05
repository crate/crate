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

import static io.crate.metadata.functions.Signature.scalar;

import java.util.Map;

import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import io.crate.expression.scalar.ScalarFunctions;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.types.DataTypes;
import io.crate.types.GeoShapeType;

/**
 * This class implements the area(geoShape) function on columns of GEO_SHAPE type.
 * Usage example:
 * <pre>
 *    SELECT area(shape)
 *    FROM table
 *    ORDER BY area(shape)
 * </pre>
 */
public final class AreaFunction {

    public static final String FUNCTION_NAME = "area";

    /**
     * Registers the area function in the {@link ScalarFunctions}.
     * It takes as input a GEO_SHAPE and produces a double value
     * designated as the area of the shape.
     *
     * @param module the {@link ScalarFunctions}
     */
    public static void register(Functions.Builder builder) {
        builder.add(
            scalar(
                FUNCTION_NAME,
                DataTypes.GEO_SHAPE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new UnaryScalar<>(
                    signature,
                    boundSignature,
                    DataTypes.GEO_SHAPE,
                    AreaFunction::getArea
                )
        );
    }

    protected static Double getArea(Object value) {

        Map<String, Object> shapeAsMap = GeoShapeType.INSTANCE.sanitizeValue(value);
        Shape shape = GeoJSONUtils.map2Shape(shapeAsMap);

        return shape.getArea(JtsSpatialContext.GEO);
    }
}

