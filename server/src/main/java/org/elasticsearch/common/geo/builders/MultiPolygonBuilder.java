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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Shape;

public class MultiPolygonBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOLYGON;

    private final List<PolygonBuilder> polygons = new ArrayList<>();

    private final Orientation orientation;

    /**
     * Build a MultiPolygonBuilder with RIGHT orientation.
     */
    public MultiPolygonBuilder() {
        this(Orientation.RIGHT);
    }

    /**
     * Build a MultiPolygonBuilder with an arbitrary orientation.
     */
    public MultiPolygonBuilder(Orientation orientation) {
        this.orientation = orientation;
    }

    /**
     * Add a shallow copy of the polygon to the multipolygon. This will apply the orientation of the
     * {@link MultiPolygonBuilder} to the polygon if polygon has different orientation.
     */
    public MultiPolygonBuilder polygon(PolygonBuilder polygon) {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder().coordinates(polygon.shell().coordinates(false)), this.orientation);
        for (LineStringBuilder hole : polygon.holes()) {
            pb.hole(hole);
        }
        this.polygons.add(pb);
        return this;
    }

    @Override
    public Shape buildS4J() {
        List<Shape> shapes = new ArrayList<>(this.polygons.size());
        if (wrapdateline) {
            for (PolygonBuilder polygon : this.polygons) {
                for (Coordinate[][] part : polygon.coordinates()) {
                    shapes.add(jtsGeometry(PolygonBuilder.polygonS4J(GEO_FACTORY, part)));
                }
            }
        } else {
            for (PolygonBuilder polygon : this.polygons) {
                shapes.add(jtsGeometry(polygon.toPolygonS4J(GEO_FACTORY)));
            }
        }
        if (shapes.size() == 1)
            return shapes.get(0);
        else
            return new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        //note: ShapeCollection is probably faster than a Multi* geom.
    }

    @Override
    public Object buildLucene() {
        List<org.apache.lucene.geo.Polygon> shapes = new ArrayList<>(this.polygons.size());
        Object poly;
        if (wrapdateline) {
            for (PolygonBuilder polygon : this.polygons) {
                poly = polygon.buildLucene();
                if (poly instanceof org.apache.lucene.geo.Polygon[]) {
                    shapes.addAll(Arrays.asList((org.apache.lucene.geo.Polygon[])poly));
                } else {
                    shapes.add((org.apache.lucene.geo.Polygon)poly);
                }
            }
        } else {
            for (int i = 0; i < this.polygons.size(); ++i) {
                PolygonBuilder pb = this.polygons.get(i);
                poly = pb.buildLucene();
                if (poly instanceof org.apache.lucene.geo.Polygon[]) {
                    shapes.addAll(Arrays.asList((org.apache.lucene.geo.Polygon[])poly));
                } else {
                    shapes.add((org.apache.lucene.geo.Polygon)poly);
                }
            }
        }
        return shapes.stream().toArray(org.apache.lucene.geo.Polygon[]::new);
    }

    @Override
    public int hashCode() {
        return Objects.hash(polygons, orientation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MultiPolygonBuilder other = (MultiPolygonBuilder) obj;
        return Objects.equals(polygons, other.polygons) &&
                Objects.equals(orientation, other.orientation);
    }
}
