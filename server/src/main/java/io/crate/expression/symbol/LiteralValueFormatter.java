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

package io.crate.expression.symbol;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.UUID;

import org.joda.time.Period;
import org.locationtech.spatial4j.shape.Point;

import io.crate.sql.Literals;

public final class LiteralValueFormatter {

    private LiteralValueFormatter() {
    }

    public static void format(Object value, StringBuilder builder) {
        if (value == null) {
            builder.append("NULL");
        } else if (value instanceof Map<?, ?> map) {
            formatMap(map, builder);
        } else if (value instanceof Iterable<?> iterable) {
            formatIterable(iterable, builder);
        } else if (value.getClass().isArray()) {
            formatArray(value, builder);
        } else if (value instanceof String || value instanceof Point) {
            builder.append(Literals.quoteStringLiteral(value.toString()));
        } else if (value instanceof Period) {
            builder.append(Literals.quoteStringLiteral(value.toString()));
            builder.append("::interval");
        } else if (value instanceof UUID uuid) {
            builder.append("'");
            builder.append(uuid.toString());
            builder.append("'");
        } else {
            builder.append(value);
        }
    }

    private static void formatIterable(Iterable<?> iterable, StringBuilder builder) {
        builder.append('[');
        var it = iterable.iterator();
        while (it.hasNext()) {
            var elem = it.next();
            format(elem, builder);
            if (it.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(']');
    }

    private static void formatMap(Map<?, ?> map, StringBuilder builder) {
        builder.append("{");
        var it = map
            .entrySet()
            .iterator();
        while (it.hasNext()) {
            var entry = it.next();
            Object identifier = entry.getKey();
            Object value = entry.getValue();
            builder
                .append('"')
                .append(identifier)
                .append('"')
                .append("=");
            format(value, builder);
            if (it.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append("}");
    }

    private static void formatArray(Object array, StringBuilder builder) {
        builder.append('[');
        for (int i = 0, length = Array.getLength(array); i < length; i++) {
            format(Array.get(array, i), builder);
            if (i + 1 < length) {
                builder.append(", ");
            }
        }
        builder.append(']');
    }
}
