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

import static org.elasticsearch.common.geo.GeoUtils.normalizeLat;
import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.geo.Line;
import org.elasticsearch.common.geo.GeoShapeType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class LineStringBuilder extends ShapeBuilder<JtsGeometry, LineStringBuilder> {
    public static final GeoShapeType TYPE = GeoShapeType.LINESTRING;

    /**
     * Construct a new LineString.
     * Per GeoJSON spec (http://geojson.org/geojson-spec.html#linestring)
     * a LineString must contain two or more coordinates
     * @param coordinates the initial list of coordinates
     * @throws IllegalArgumentException if there are less then two coordinates defined
     */
    public LineStringBuilder(List<Coordinate> coordinates) {
        super(coordinates);
        if (coordinates.size() < 2) {
            throw new IllegalArgumentException(
                "invalid number of points in LineString (found [" + coordinates.size() + "] - must be >= 2)");
        }
    }

    public LineStringBuilder(CoordinatesBuilder coordinates) {
        this(coordinates.build());
    }

    /**
     * Closes the current lineString by adding the starting point as the end point.
     * This will have no effect if starting and end point are already the same.
     */
    public LineStringBuilder close() {
        Coordinate start = coordinates.get(0);
        Coordinate end = coordinates.get(coordinates.size() - 1);
        if (start.x != end.x || start.y != end.y) {
            coordinates.add(start);
        }
        return this;
    }

    @Override
    public JtsGeometry buildS4J() {
        Coordinate[] coordinates = this.coordinates.toArray(new Coordinate[this.coordinates.size()]);
        Geometry geometry;
        if (wrapdateline) {
            ArrayList<LineString> strings = decomposeS4J(GEO_FACTORY, coordinates, new ArrayList<LineString>());
            if (strings.size() == 1) {
                geometry = strings.get(0);
            } else {
                LineString[] linestrings = strings.toArray(new LineString[strings.size()]);
                geometry = GEO_FACTORY.createMultiLineString(linestrings);
            }

        } else {
            geometry = GEO_FACTORY.createLineString(coordinates);
        }
        return jtsGeometry(geometry);
    }

    @Override
    public Object buildLucene() {
        // decompose linestrings crossing dateline into array of Lines
        Coordinate[] coordinates = this.coordinates.toArray(new Coordinate[this.coordinates.size()]);
        if (wrapdateline) {
            ArrayList<Line> linestrings = decomposeLucene(coordinates, new ArrayList<>());
            if (linestrings.size() == 1) {
                return linestrings.get(0);
            } else {
                return linestrings.toArray(new Line[linestrings.size()]);
            }
        }
        return new Line(Arrays.stream(coordinates).mapToDouble(i -> normalizeLat(i.y)).toArray(),
            Arrays.stream(coordinates).mapToDouble(i -> normalizeLon(i.x)).toArray());
    }

    static ArrayList<LineString> decomposeS4J(GeometryFactory factory, Coordinate[] coordinates, ArrayList<LineString> strings) {
        for (Coordinate[] part : decompose(+DATELINE, coordinates)) {
            for (Coordinate[] line : decompose(-DATELINE, part)) {
                strings.add(factory.createLineString(line));
            }
        }
        return strings;
    }

    static ArrayList<Line> decomposeLucene(Coordinate[] coordinates, ArrayList<Line> lines) {
        for (Coordinate[] part : decompose(+DATELINE, coordinates)) {
            for (Coordinate[] line : decompose(-DATELINE, part)) {
                lines.add(new Line(Arrays.stream(line).mapToDouble(i -> normalizeLat(i.y)).toArray(),
                    Arrays.stream(line).mapToDouble(i -> normalizeLon(i.x)).toArray()));
            }
        }
        return lines;
    }

    /**
     * Decompose a linestring given as array of coordinates at a vertical line.
     *
     * @param dateline x-axis intercept of the vertical line
     * @param coordinates coordinates forming the linestring
     * @return array of linestrings given as coordinate arrays
     */
    private static Coordinate[][] decompose(double dateline, Coordinate[] coordinates) {
        int offset = 0;
        ArrayList<Coordinate[]> parts = new ArrayList<>();

        double shift = coordinates[0].x > DATELINE ? DATELINE : (coordinates[0].x < -DATELINE ? -DATELINE : 0);

        for (int i = 1; i < coordinates.length; i++) {
            double t = intersection(coordinates[i - 1], coordinates[i], dateline);
            if (!Double.isNaN(t)) {
                Coordinate[] part;
                if (t < 1) {
                    part = Arrays.copyOfRange(coordinates, offset, i + 1);
                    part[part.length - 1] = Edge.position(coordinates[i - 1], coordinates[i], t);
                    coordinates[offset + i - 1] = Edge.position(coordinates[i - 1], coordinates[i], t);
                    shift(shift, part);
                    offset = i - 1;
                    shift = coordinates[i].x > DATELINE ? DATELINE : (coordinates[i].x < -DATELINE ? -DATELINE : 0);
                } else {
                    part = shift(shift, Arrays.copyOfRange(coordinates, offset, i + 1));
                    offset = i;
                }
                parts.add(part);
            }
        }

        if (offset == 0) {
            parts.add(shift(shift, coordinates));
        } else if (offset < coordinates.length - 1) {
            Coordinate[] part = Arrays.copyOfRange(coordinates, offset, coordinates.length);
            parts.add(shift(shift, part));
        }
        return parts.toArray(new Coordinate[parts.size()][]);
    }

    private static Coordinate[] shift(double shift, Coordinate...coordinates) {
        if (shift != 0) {
            for (int j = 0; j < coordinates.length; j++) {
                coordinates[j] = new Coordinate(coordinates[j].x - 2 * shift, coordinates[j].y);
            }
        }
        return coordinates;
    }
}
