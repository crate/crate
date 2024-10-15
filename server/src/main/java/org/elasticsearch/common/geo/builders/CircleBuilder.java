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

import java.util.Objects;

import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.xcontent.ParseField;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Circle;

public class CircleBuilder extends ShapeBuilder<Circle, CircleBuilder> {

    public static final ParseField FIELD_RADIUS = new ParseField("radius");
    public static final GeoShapeType TYPE = GeoShapeType.CIRCLE;

    private DistanceUnit unit = DistanceUnit.DEFAULT;
    private double radius;
    private Coordinate center;

    /**
     * Creates a circle centered at [0.0, 0.0].
     * Center can be changed by calling {@link #center(Coordinate)} later.
     */
    public CircleBuilder() {
        this.center = ZERO_ZERO;
    }

    /**
     * Set the center of the circle
     *
     * @param center coordinate of the circles center
     * @return this
     */
    public CircleBuilder center(Coordinate center) {
        this.center = center;
        return this;
    }

    /**
     * Set the radius of the circle
     * @param radius radius of the circle (see {@link org.elasticsearch.common.unit.DistanceUnit.Distance})
     * @return this
     */
    public CircleBuilder radius(Distance radius) {
        return radius(radius.value, radius.unit);
    }

    /**
     * Set the radius of the circle
     * @param radius value of the circles radius
     * @param unit unit of the radius value (see {@link DistanceUnit})
     * @return this
     */
    public CircleBuilder radius(double radius, DistanceUnit unit) {
        this.unit = unit;
        this.radius = radius;
        return this;
    }

    @Override
    public Circle buildS4J() {
        return SHAPE_FACTORY.circle(center.x, center.y, 360 * radius / unit.getEarthCircumference());
    }

    @Override
    public Object buildLucene() {
        throw new UnsupportedOperationException("CIRCLE geometry is not supported");
    }

    @Override
    public int hashCode() {
        return Objects.hash(center, radius, unit.ordinal());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CircleBuilder other = (CircleBuilder) obj;
        return Objects.equals(center, other.center) &&
                Objects.equals(radius, other.radius) &&
                Objects.equals(unit.ordinal(), other.unit.ordinal());
    }
}
