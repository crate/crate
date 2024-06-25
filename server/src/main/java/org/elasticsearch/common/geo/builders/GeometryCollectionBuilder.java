/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo.builders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.XShapeCollection;
import org.locationtech.spatial4j.shape.Shape;

public class GeometryCollectionBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.GEOMETRYCOLLECTION;

    /**
     * List of shapes. Package scope for testing.
     */
    final List<ShapeBuilder> shapes = new ArrayList<>();

    /**
     * Build and empty GeometryCollectionBuilder.
     */
    public GeometryCollectionBuilder() {
    }

    public GeometryCollectionBuilder shape(ShapeBuilder shape) {
        this.shapes.add(shape);
        return this;
    }

    @Override
    public int numDimensions() {
        if (shapes == null || shapes.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " +
                "GeometryCollection has not yet been initialized");
        }
        return shapes.get(0).numDimensions();
    }

    @Override
    public Shape buildS4J() {

        List<Shape> shapes = new ArrayList<>(this.shapes.size());

        for (ShapeBuilder shape : this.shapes) {
            shapes.add(shape.buildS4J());
        }

        if (shapes.size() == 1)
            return shapes.get(0);
        else
            return new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        //note: ShapeCollection is probably faster than a Multi* geom.
    }

    @Override
    public Object buildLucene() {
        List<Object> shapes = new ArrayList<>(this.shapes.size());

        for (ShapeBuilder shape : this.shapes) {
            Object o = shape.buildLucene();
            if (o.getClass().isArray()) {
                shapes.addAll(Arrays.asList((Object[])o));
            } else {
                shapes.add(o);
            }
        }

        if (shapes.size() == 1) {
            return shapes.get(0);
        }
        return shapes.toArray(new Object[shapes.size()]);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shapes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GeometryCollectionBuilder other = (GeometryCollectionBuilder) obj;
        return Objects.equals(shapes, other.shapes);
    }
}
