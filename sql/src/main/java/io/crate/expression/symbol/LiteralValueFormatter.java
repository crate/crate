/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.symbol;

import io.crate.core.collections.Sorted;
import io.crate.sql.Literals;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LiteralValueFormatter {

    public static final LiteralValueFormatter INSTANCE = new LiteralValueFormatter();

    @SuppressWarnings("unchecked")
    public void format(Object value, StringBuilder builder) {
        if (value == null) {
            builder.append("NULL");
        } else if (value instanceof Map) {
            formatMap((Map<String, Object>) value, builder);
        } else if (value instanceof Set) {
            formatIterable(Sorted.sortRecursive((Collection) value), builder);
        } else if (value instanceof Collection) {
            formatIterable((Iterable<?>) value, builder);
        } else if (value instanceof Object[]) {
            formatIterable(Arrays.asList((Object[]) value), builder);
        } else if (value.getClass().isArray()) {
            formatArray(value, builder);
        } else if (value instanceof CharSequence || value instanceof Character || value instanceof BytesRef) {
            builder.append(Literals.quoteStringLiteral(BytesRefs.toString(value)));
        } else {
            builder.append(value.toString());
        }

    }

    private void formatIterable(Iterable<?> iterable, StringBuilder builder) {
        builder.append('[');
        boolean first = true;
        for (Object elem : iterable) {
            if (!first) {
                builder.append(", ");
            } else {
                first = false;
            }
            format(elem, builder);
        }
        builder.append(']');
    }

    private void formatMap(Map<String, Object> map, StringBuilder builder) {
        builder.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : Sorted.sortRecursive(map, true).entrySet()) {
            if (!first) {
                builder.append(", ");
            } else {
                first = false;
            }
            formatIdentifier(entry.getKey(), builder);
            builder.append("=");
            format(entry.getValue(), builder);
        }
        builder.append("}");
    }

    private void formatIdentifier(String identifier, StringBuilder builder) {
        builder.append('"').append(identifier).append('"');
    }

    private void formatArray(Object array, StringBuilder builder) {
        builder.append('[');
        boolean first = true;
        for (int i = 0, length = Array.getLength(array); i < length; i++) {
            if (!first) {
                builder.append(", ");
            } else {
                first = false;
            }
            format(Array.get(array, i), builder);
        }
        builder.append(']');
    }
}
