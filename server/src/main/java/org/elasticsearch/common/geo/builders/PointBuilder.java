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

import org.elasticsearch.common.geo.GeoShapeType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Point;

public class PointBuilder extends ShapeBuilder<Point, PointBuilder> {
    public static final GeoShapeType TYPE = GeoShapeType.POINT;

    /**
     * Create a point at [0.0,0.0]
     */
    public PointBuilder() {
        super();
        this.coordinates.add(ZERO_ZERO);
    }

    public PointBuilder coordinate(Coordinate coordinate) {
        this.coordinates.set(0, coordinate);
        return this;
    }

    @Override
    public Point buildS4J() {
        return SHAPE_FACTORY.pointXY(coordinates.get(0).x, coordinates.get(0).y);
    }

    @Override
    public org.apache.lucene.geo.Point buildLucene() {
        return new org.apache.lucene.geo.Point(coordinates.get(0).y, coordinates.get(0).x);
    }

    @Override
    public int numDimensions() {
        return Double.isNaN(coordinates.get(0).z) ? 2 : 3;
    }
}
