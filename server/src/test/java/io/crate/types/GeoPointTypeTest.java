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

package io.crate.types;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GeoPointTypeTest extends ESTestCase {

    @Test
    public void testStreaming() throws Throwable {
        Point p1 = new PointImpl(41.2, -37.4, JtsSpatialContext.GEO);

        BytesStreamOutput out = new BytesStreamOutput();
        DataTypes.GEO_POINT.writeValueTo(out, p1);

        StreamInput in = out.bytes().streamInput();
        Point p2 = DataTypes.GEO_POINT.readValueFrom(in);

        assertThat(p1, equalTo(p2));
    }

    @Test
    public void test_sanitize_list_of_doubles_value() {
        Point value = DataTypes.GEO_POINT.sanitizeValue(List.of(1d, 2d));
        assertThat(value.getX(), is(1.0d));
        assertThat(value.getY(), is(2.0d));
    }

    @Test
    public void testWktToGeoPointValue() throws Exception {
        Point value = DataTypes.GEO_POINT.implicitCast("POINT(1 2)");
        assertThat(value.getX(), is(1.0d));
        assertThat(value.getY(), is(2.0d));
    }

    @Test
    public void testInvalidWktToGeoPointValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"POINT(54.321 -123.456)\" to geo_point." +
            " Bad Y value -123.456 is not in boundary Rect(minX=-180.0,maxX=180.0,minY=-90.0,maxY=90.0)");
        DataTypes.GEO_POINT.implicitCast("POINT(54.321 -123.456)");
    }

    @Test
    public void testValueConversionFromList() throws Exception {
        Point value = DataTypes.GEO_POINT.implicitCast(List.of(10.0, 20.2));
        assertThat(value.getX(), is(10.0d));
        assertThat(value.getY(), is(20.2d));
    }

    @Test
    public void testConversionFromObjectArrayOfIntegers() throws Exception {
        Point value = DataTypes.GEO_POINT.implicitCast(new Object[]{1, 2});
        assertThat(value.getX(), is(1.0));
        assertThat(value.getY(), is(2.0));
    }

    @Test
    public void testConversionFromIntegerArray() throws Exception {
        Point value = DataTypes.GEO_POINT.implicitCast(new Integer[]{1, 2});
        assertThat(value.getX(), is(1.0));
        assertThat(value.getY(), is(2.0));
    }

    @Test
    public void test_cast_double_geo_point_value_with_invalid_latitude_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Failed to validate geo point [lon=54.321000, lat=-123.456000], not a valid location.");
        DataTypes.GEO_POINT.implicitCast(new Double[]{54.321, -123.456});
    }

    @Test
    public void test_cast_double_geo_point_value_with_invalid_longitude_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Failed to validate geo point [lon=-187.654000, lat=123.456000], not a valid location.");
        DataTypes.GEO_POINT.implicitCast(new Double[]{-187.654, 123.456});
    }
}
