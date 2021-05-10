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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.geo.GeoJSONUtils;
import io.crate.types.DataTypes;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * for where within(shape1, shape2) = [ true | false ]
 */
class WithinQuery implements FunctionToQuery, InnerFunctionToQuery {

    @Override
    public Query apply(Function parent, Function inner, LuceneQueryBuilder.Context context) {
        FunctionLiteralPair outerPair = new FunctionLiteralPair(parent);
        if (!outerPair.isValid()) {
            return null;
        }
        Query query = getQuery(inner, context);
        if (query == null) return null;
        boolean negate = !(Boolean) outerPair.input().value();
        if (negate) {
            return Queries.not(query);
        } else {
            return query;
        }
    }

    private Query getQuery(Function inner, LuceneQueryBuilder.Context context) {
        RefAndLiteral innerPair = RefAndLiteral.of(inner);
        if (innerPair == null) {
            return null;
        }
        if (innerPair.reference().valueType().equals(DataTypes.GEO_SHAPE)) {
            // we have within('POINT(0 0)', shape_column)
            return LuceneQueryBuilder.genericFunctionFilter(inner, context);
        }
        GeoPointFieldMapper.GeoPointFieldType geoPointFieldType = getGeoPointFieldType(
            innerPair.reference().column().fqn(),
            context.mapperService);

        Map<String, Object> geoJSON = DataTypes.GEO_SHAPE.implicitCast(innerPair.literal().value());
        Geometry geometry;
        Shape shape = GeoJSONUtils.map2Shape(geoJSON);
        if (shape instanceof ShapeCollection) {
            int i = 0;
            ShapeCollection<Shape> collection = (ShapeCollection) shape;
            org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[collection.size()];
            for (Shape s : collection.getShapes()) {
                Geometry subGeometry = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(s);
                if (subGeometry instanceof org.locationtech.jts.geom.Polygon) {
                    polygons[i++] = (org.locationtech.jts.geom.Polygon) subGeometry;
                } else {
                    throw new InvalidShapeException("Shape collection must contain only Polygon shapes.");
                }
            }
            GeometryFactory geometryFactory = JtsSpatialContext.GEO.getShapeFactory().getGeometryFactory();
            geometry = geometryFactory.createMultiPolygon(polygons);
        } else {
            geometry = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(shape);
        }

        return getPolygonQuery(geometry, geoPointFieldType);
    }

    private static Query getPolygonQuery(Geometry geometry,
                                         GeoPointFieldMapper.GeoPointFieldType fieldType) {
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

        return LatLonPoint.newPolygonQuery(fieldType.name(), new Polygon(lats, lons));
    }

    private static GeoPointFieldMapper.GeoPointFieldType getGeoPointFieldType(String fieldName, MapperService mapperService) {
        MappedFieldType fieldType = mapperService.fullName(fieldName);
        if (fieldType == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "column \"%s\" doesn't exist", fieldName));
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "column \"%s\" isn't of type geo_point", fieldName));
        }
        return (GeoPointFieldMapper.GeoPointFieldType) fieldType;
    }

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        return getQuery(input, context);
    }
}
