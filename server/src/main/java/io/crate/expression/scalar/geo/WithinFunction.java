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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.SpatialRelation;

import io.crate.data.Input;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.geo.GeoJSONUtils;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

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

    private WithinFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 : "number of args must be 2";
        return evaluate(args[0], args[1]);
    }

    public Boolean evaluate(Input<?> leftInput, Input<?> rightInput) {
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
        if (left instanceof Point point) {
            shape = SpatialContext.GEO.getShapeFactory().pointXY(point.getX(), point.getY());
        } else if (left instanceof Double[] values) {
            shape = SpatialContext.GEO.getShapeFactory().pointXY(values[0], values[1]);
        } else if (left instanceof String str) {
            shape = GeoJSONUtils.wkt2Shape(str);
        } else {
            shape = GeoJSONUtils.map2Shape((Map<String, Object>) left);
        }
        return shape;
    }

    @SuppressWarnings("unchecked")
    private Shape parseRightShape(Object right) {
        return (right instanceof String str) ?
            GeoJSONUtils.wkt2Shape(str) :
            GeoJSONUtils.map2Shape((Map<String, Object>) right);
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
            return Literal.of(evaluate((Input<?>) left, (Input<?>) right));
        }

        return symbol;
    }

    @Override
    public Query toQuery(Function parent, Function inner, Context context) {
        // within(p, pointOrShape) = [ true | false ]
        if (parent.name().equals(EqOperator.NAME)
                && parent.arguments().get(1) instanceof Literal<?> eqLiteral
                && inner.arguments().get(0) instanceof Reference ref
                && inner.arguments().get(1) instanceof Literal<?> pointOrShape) {

            Query query = toQuery(ref, pointOrShape);
            if (query == null) {
                return null;
            }
            Boolean isWithin = (Boolean) eqLiteral.value();
            if (isWithin == null) {
                // Need to fallback to generic function filter because `null = null` is `null`
                // and depending on parent queries that could turn into a match.
                return null;
            }
            return isWithin ? query : Queries.not(query);
        }
        return null;
    }

    @Override
    public Query toQuery(Reference ref, Literal<?> literal) {
        if (ref.valueType().equals(DataTypes.GEO_SHAPE)) {
            // Can only optimize on point columns, not on shapes
            return null;
        }
        Map<String, Object> geoJSON = DataTypes.GEO_SHAPE.implicitCast(literal.value());
        Geometry geometry;
        Shape shape = GeoJSONUtils.map2Shape(geoJSON);
        if (shape instanceof ShapeCollection<?> collection) {
            int i = 0;
            org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[collection.size()];
            for (Shape s : collection.getShapes()) {
                Geometry subGeometry = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(s);
                if (subGeometry instanceof org.locationtech.jts.geom.Polygon polygon) {
                    polygons[i++] = polygon;
                } else {
                    throw new InvalidShapeException("Shape collection must contain only Polygon shapes.");
                }
            }
            GeometryFactory geometryFactory = JtsSpatialContext.GEO.getShapeFactory().getGeometryFactory();
            geometry = geometryFactory.createMultiPolygon(polygons);
        } else {
            geometry = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(shape);
        }

        return getPolygonQuery(ref.storageIdent(), geometry);
    }

    private static Query getPolygonQuery(String column, Geometry geometry) {
        Coordinate[] coordinates = geometry.getCoordinates();
        // close the polygon shape if startpoint != endpoint
        if (!CoordinateArrays.isRing(coordinates)) {
            coordinates = Arrays.copyOf(coordinates, coordinates.length + 1);
            coordinates[coordinates.length - 1] = coordinates[0];
        }

        final double[] lats = new double[coordinates.length];
        final double[] lons = new double[coordinates.length];
        for (int i = 0; i < coordinates.length; i++) {
            lats[i] = coordinates[i].y;
            lons[i] = coordinates[i].x;
        }
        return LatLonPoint.newPolygonQuery(column, new Polygon(lats, lons));
    }
}
