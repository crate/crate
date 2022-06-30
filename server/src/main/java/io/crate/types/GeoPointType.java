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

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;

public class GeoPointType extends DataType<Point> implements Streamer<Point>, FixedWidthType {

    public static final int ID = 13;
    public static final GeoPointType INSTANCE = new GeoPointType();
    private static StorageSupport<Point> STORAGE = new StorageSupport<>(true, true, null);

    private GeoPointType() {
    }

    public static final WKTReader WKT_READER = (WKTReader) JtsSpatialContext.GEO.getFormats().getWktReader();

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.GEO_POINT;
    }

    @Override
    public String getName() {
        return "geo_point";
    }

    @Override
    public Streamer<Point> streamer() {
        return this;
    }

    @Override
    public Point implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Point) {
            return ((Point) value);
        } else if (value instanceof Double[]) {
            Double[] doubles = (Double[]) value;
            checkLengthIs2(doubles.length);
            ensurePointsInRange(doubles[0], doubles[1]);
            return new PointImpl(doubles[0], doubles[1], JtsSpatialContext.GEO);
        } else if (value instanceof Object[]) {
            Object[] values = (Object[]) value;
            checkLengthIs2(values.length);
            PointImpl point = new PointImpl(
                ((Number) values[0]).doubleValue(),
                ((Number) values[1]).doubleValue(),
                JtsSpatialContext.GEO);
            ensurePointsInRange(point.getX(), point.getY());
            return point;
        } else if (value instanceof String) {
            return pointFromString((String) value);
        } else if (value instanceof List) {
            List<?> values = (List<?>) value;
            checkLengthIs2(values.size());
            PointImpl point = new PointImpl(
                ((Number) values.get(0)).doubleValue(),
                ((Number) values.get(1)).doubleValue(),
                JtsSpatialContext.GEO);
            ensurePointsInRange(point.getX(), point.getY());
            return point;
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Point sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof List) {
            List<?> values = (List<?>) value;
            checkLengthIs2(values.size());
            PointImpl point = new PointImpl(
                ((Number) values.get(0)).doubleValue(),
                ((Number) values.get(1)).doubleValue(),
                JtsSpatialContext.GEO);
            ensurePointsInRange(point.getX(), point.getY());
            return point;
        } else {
            return (Point) value;
        }
    }

    private void ensurePointsInRange(double x, double y) {
        if (!arePointsInRange(x, y)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Failed to validate geo point [lon=%f, lat=%f], not a valid location.",
                x, y));
        }
    }

    private static boolean arePointsInRange(double x, double y) {
        return x >= -180.0d && x <= 180.0d && y >= -90.0d && y <= 90.0d;
    }

    private static void checkLengthIs2(int actualLength) {
        if (actualLength != 2) {
            throw new IllegalArgumentException(
                "The value of a GeoPoint must be a double array with 2 items, not " + actualLength);
        }
    }

    private static Point pointFromString(String value) {
        try {
            return (Point) WKT_READER.parse(value);
        } catch (ParseException | InvalidShapeException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert \"%s\" to geo_point. %s", value, e.getLocalizedMessage()), e);
        }
    }

    @Override
    public int compare(Point val1, Point val2) {
        if (val1 == null) {
            return -1;
        }
        if (val2 == null) {
            return 1;
        }
        // this is probably not really correct, but should be sufficient for the compareValueTo use case
        // (which is ordering and equality check)
        int latComp = Double.compare(val1.getX(), val2.getX());
        if (latComp != 0) {
            return latComp;
        }
        return Double.compare(val1.getY(), val2.getY());
    }

    @Override
    public Point readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new PointImpl(in.readDouble(), in.readDouble(), JtsSpatialContext.GEO);
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, Point point) throws IOException {
        if (point == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeDouble(point.getX());
            out.writeDouble(point.getY());
        }
    }

    @Override
    public int fixedSize() {
        return 40; // 2x double + array overhead
    }

    @Override
    public boolean isConvertableTo(DataType<?> other, boolean explicitCast) {
        if (other.id() == id()
            || other instanceof ArrayType && ((ArrayType<?>) other).innerType().equals(DataTypes.DOUBLE)) {
            return true;
        }
        return super.isConvertableTo(other, explicitCast);
    }

    @Override
    public StorageSupport<Point> storageSupport() {
        return STORAGE;
    }
}
