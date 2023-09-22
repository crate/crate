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

import io.crate.sql.Literals;
import org.joda.time.Period;
import org.locationtech.spatial4j.shape.Point;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

public class LiteralValueFormatter {

    private static final LiteralValueFormatter INSTANCE = new LiteralValueFormatter();

    public static void format(Object value, StringBuilder builder) {
        INSTANCE.formatValue(value, builder);
    }

    private LiteralValueFormatter() {
    }

    @SuppressWarnings("unchecked")
    public void formatValue(Object value, StringBuilder builder) {
        if (value == null) {
            builder.append("NULL");
        } else if (value instanceof Map) {
            formatMap((Map<String, Object>) value, builder);
        } else if (value instanceof Collection) {
            formatIterable((Iterable<?>) value, builder);
        } else if (value.getClass().isArray()) {
            formatArray(value, builder);
        } else if (value instanceof String || value instanceof Point) {
            builder.append(Literals.quoteStringLiteral(value.toString()));
        } else if (value instanceof Period) {
            builder.append(Literals.quoteStringLiteral(value.toString()));
            builder.append("::interval");
        } else if (value instanceof Long
                   && ((Long) value <= Integer.MAX_VALUE
                   || (Long) value >= Integer.MIN_VALUE)) {
            builder.append(value.toString());
            builder.append("::bigint");
        } else {
            builder.append(value);
        }
    }

    private void formatIterable(Iterable<?> iterable, StringBuilder builder) {
        builder.append('[');
        var it = iterable.iterator();
        while (it.hasNext()) {
            var elem = it.next();
            formatValue(elem, builder);
            if (it.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(']');
    }

    private void formatMap(Map<String, Object> map, StringBuilder builder) {
        builder.append("{");
        var it = map
            .entrySet()
            .stream()
            .iterator();
        while (it.hasNext()) {
            var entry = it.next();
            formatIdentifier(entry.getKey(), builder);
            builder.append("=");
            formatValue(entry.getValue(), builder);
            if (it.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append("}");
    }

    private void formatIdentifier(String identifier, StringBuilder builder) {
        builder.append('"').append(identifier).append('"');
    }

    private void formatArray(Object array, StringBuilder builder) {
        builder.append('[');
        for (int i = 0, length = Array.getLength(array); i < length; i++) {
            formatValue(Array.get(array, i), builder);
            if (i + 1 < length) {
                builder.append(", ");
            }
        }
        builder.append(']');
    }
}
