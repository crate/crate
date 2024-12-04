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
import java.util.List;

import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.XShapeCollection;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Point;

public class MultiPointBuilder extends ShapeBuilder<XShapeCollection<Point>, MultiPointBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOINT;

    /**
     * Create a new {@link MultiPointBuilder}.
     * @param coordinates needs at least two coordinates to be valid, otherwise will throw an exception
     */
    public MultiPointBuilder(List<Coordinate> coordinates) {
        super(coordinates);
    }

    @Override
    public XShapeCollection<Point> buildS4J() {
        //Could wrap JtsGeometry but probably slower due to conversions to/from JTS in relate()
        //MultiPoint geometry = FACTORY.createMultiPoint(points.toArray(new Coordinate[points.size()]));
        List<Point> shapes = new ArrayList<>(coordinates.size());
        for (Coordinate coord : coordinates) {
            shapes.add(SHAPE_FACTORY.pointXY(coord.x, coord.y));
        }
        XShapeCollection<Point> multiPoints = new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        multiPoints.setPointsOnly(true);
        return multiPoints;
    }

    @Override
    public org.apache.lucene.geo.Point[] buildLucene() {
        org.apache.lucene.geo.Point[] points = new org.apache.lucene.geo.Point[coordinates.size()];
        for (int i = 0; i < coordinates.size(); ++i) {
            Coordinate coord = coordinates.get(i);
            points[i] = new org.apache.lucene.geo.Point(coord.y, coord.x);
        }
        return points;
    }
}
