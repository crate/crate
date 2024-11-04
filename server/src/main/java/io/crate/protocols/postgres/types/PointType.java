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

package io.crate.protocols.postgres.types;

import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

import org.jetbrains.annotations.NotNull;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

public final class PointType extends PGType<Point> {

    public static final PointType INSTANCE = new PointType();
    static final int OID = 600;

    private static final int TYPE_LEN = 16;
    private static final int TYPE_MOD = -1;

    PointType() {
        super(OID, TYPE_LEN, TYPE_MOD, "point");
    }

    @Override
    public int typArray() {
        return 1017;
    }

    @Override
    public String typeCategory() {
        return TypeCategory.GEOMETRIC.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public Regproc typSend() {
        return Regproc.of("point_send");
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of("point_recv");
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @NotNull Point point) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeDouble(point.getX());
        buffer.writeDouble(point.getY());
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Point readBinaryValue(ByteBuf buffer, int valueLength) {
        double x = buffer.readDouble();
        double y = buffer.readDouble();
        return new PointImpl(x, y, JtsSpatialContext.GEO);
    }

    @Override
    byte[] encodeAsUTF8Text(@NotNull Point point) {
        return ('(' + String.valueOf(point.getX()) + ',' + point.getY() + ')').getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Point decodeUTF8Text(byte[] bytes) {
        String value = new String(bytes, StandardCharsets.UTF_8);
        StringTokenizer tokenizer = new StringTokenizer(value, ",()");
        double x;
        double y;
        if (tokenizer.hasMoreTokens()) {
            x = JavaDoubleParser.parseDouble(tokenizer.nextToken());
        } else {
            throw new IllegalArgumentException("Cannot parse input as point: " + value + " expected a point in format: (x, y)");
        }
        if (tokenizer.hasMoreTokens()) {
            y = JavaDoubleParser.parseDouble(tokenizer.nextToken());
        } else {
            throw new IllegalArgumentException("Cannot parse input as point: " + value + " expected a point in format: (x, y)");
        }
        return new PointImpl(x, y, JtsSpatialContext.GEO);
    }
}
