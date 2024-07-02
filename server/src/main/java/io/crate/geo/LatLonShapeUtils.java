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

package io.crate.geo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;

import io.crate.exceptions.UnsupportedFeatureException;

public class LatLonShapeUtils {

    public static void createIndexableFields(String name, Object luceneShape, Consumer<? super IndexableField> addField) {
        if (luceneShape instanceof Point point) {
            addFields(LatLonShape.createIndexableFields(name, point.getLat(), point.getLon()), addField);
        } else if (luceneShape instanceof Line line) {
            addFields(LatLonShape.createIndexableFields(name, line), addField);
        } else if (luceneShape instanceof Polygon polygon) {
            addFields(LatLonShape.createIndexableFields(name, polygon), addField);
        } else if (luceneShape instanceof Point[] points) {
            for (Point point : points) {
                addFields(LatLonShape.createIndexableFields(name, point.getLat(), point.getLon()), addField);
            }
        } else if (luceneShape instanceof Line[] lines) {
            for (int i = 0; i < lines.length; ++i) {
                addFields(LatLonShape.createIndexableFields(name, lines[i]), addField);
            }
        } else if (luceneShape instanceof Polygon[] polygons) {
            for (int i = 0; i < polygons.length; ++i) {
                addFields(LatLonShape.createIndexableFields(name, polygons[i]), addField);
            }
        } else if (luceneShape instanceof Object[] collection) {
            for (Object o : collection) {
                createIndexableFields(name, o, addField);
            }
        } else {
            throw new IllegalArgumentException("Invalid shape type found [" + luceneShape.getClass() + "] while indexing shape");
        }
    }

    private static void addFields(Field[] indexableFields, Consumer<? super IndexableField> addField) {
        for (var field : indexableFields) {
            addField.accept(field);
        }
    }

    public static Query newLatLonShapeQuery(String fieldName, ShapeField.QueryRelation relation, Object queryShape) {
        List<LatLonGeometry> geometries = collectGeometries(fieldName, queryShape);
        if (relation == ShapeField.QueryRelation.WITHIN) {
            if (geometries.stream().anyMatch(geometry -> geometry instanceof Line)) {
                // LatLonShapeQuery does not support WITHIN queries with line geometries.
                throw new UnsupportedFeatureException("WITHIN queries with line geometries are not supported");
            }
        }
        return LatLonShape.newGeometryQuery(fieldName, relation, geometries.toArray(new LatLonGeometry[0]));
    }

    private static List<LatLonGeometry> collectGeometries(String fieldName, Object queryShape) {
        if (queryShape instanceof Line[] lines) {
            return Arrays.asList(lines);
        } else if (queryShape instanceof Polygon[] polygons) {
            return Arrays.asList(polygons);
        } else if (queryShape instanceof Line line) {
            return Collections.singletonList(line);
        } else if (queryShape instanceof Polygon polygon) {
            return Collections.singletonList(polygon);
        } else if (queryShape instanceof Point[] points) {
            return Arrays.asList(points);
        } else if (queryShape instanceof Point point) {
            return Collections.singletonList(point);
        } else if (queryShape instanceof Object[] collection) {
            List<LatLonGeometry> geometries = new ArrayList<>();
            for (Object shape : collection) {
                geometries.addAll(collectGeometries(fieldName, shape));
            }
            return geometries;
        } else {
            throw new IllegalArgumentException("Unknown shape for field [" + fieldName + "] found");
        }
    }
}
