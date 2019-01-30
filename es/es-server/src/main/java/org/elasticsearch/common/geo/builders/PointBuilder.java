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
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.jts.geom.Coordinate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class PointBuilder extends ShapeBuilder<Point, PointBuilder> {
    public static final GeoShapeType TYPE = GeoShapeType.POINT;

    /**
     * Create a point at [0.0,0.0]
     */
    public PointBuilder() {
        super();
        this.coordinates.add(ZERO_ZERO);
    }

    public PointBuilder(double lon, double lat) {
        //super(new ArrayList<>(1));
        super();
        this.coordinates.add(new Coordinate(lon, lat));
    }

    public PointBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public PointBuilder coordinate(Coordinate coordinate) {
        this.coordinates.set(0, coordinate);
        return this;
    }

    public double longitude() {
        return coordinates.get(0).x;
    }

    public double latitude() {
        return coordinates.get(0).y;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
       builder.startObject();
       builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
       builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
       toXContent(builder, coordinates.get(0));
       return builder.endObject();
    }

    @Override
    public Point build() {
        return SPATIAL_CONTEXT.makePoint(coordinates.get(0).x, coordinates.get(0).y);
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        return Double.isNaN(coordinates.get(0).z) ? 2 : 3;
    }
}
