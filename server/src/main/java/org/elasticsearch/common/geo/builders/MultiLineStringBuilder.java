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
import java.util.Iterator;
import java.util.Objects;

import org.apache.lucene.geo.Line;
import org.elasticsearch.common.geo.GeoShapeType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class MultiLineStringBuilder extends ShapeBuilder<JtsGeometry, MultiLineStringBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTILINESTRING;

    private final ArrayList<LineStringBuilder> lines = new ArrayList<>();

    public MultiLineStringBuilder() {
        super();
    }

    public MultiLineStringBuilder linestring(LineStringBuilder line) {
        this.lines.add(line);
        return this;
    }

    @Override
    public JtsGeometry buildS4J() {
        final Geometry geometry;
        if (wrapdateline) {
            ArrayList<LineString> parts = new ArrayList<>();
            for (LineStringBuilder line : lines) {
                LineStringBuilder.decomposeS4J(GEO_FACTORY, line.coordinates(false), parts);
            }
            if (parts.size() == 1) {
                geometry = parts.get(0);
            } else {
                LineString[] lineStrings = parts.toArray(new LineString[parts.size()]);
                geometry = GEO_FACTORY.createMultiLineString(lineStrings);
            }
        } else {
            LineString[] lineStrings = new LineString[lines.size()];
            Iterator<LineStringBuilder> iterator = lines.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                lineStrings[i] = GEO_FACTORY.createLineString(iterator.next().coordinates(false));
            }
            geometry = GEO_FACTORY.createMultiLineString(lineStrings);
        }
        return jtsGeometry(geometry);
    }

    @Override
    public Object buildLucene() {
        if (wrapdateline) {
            ArrayList<Line> parts = new ArrayList<>();
            for (LineStringBuilder line : lines) {
                LineStringBuilder.decomposeLucene(line.coordinates(false), parts);
            }
            if (parts.size() == 1) {
                return parts.get(0);
            }
            return parts.toArray(new Line[parts.size()]);
        }
        Line[] linestrings = new Line[lines.size()];
        for (int i = 0; i < lines.size(); ++i) {
            LineStringBuilder lsb = lines.get(i);
            linestrings[i] = new Line(lsb.coordinates.stream().mapToDouble(c -> c.y).toArray(),
                lsb.coordinates.stream().mapToDouble(c -> c.x).toArray());
        }
        return linestrings;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lines);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MultiLineStringBuilder other = (MultiLineStringBuilder) obj;
        return Objects.equals(lines, other.lines);
    }
}
