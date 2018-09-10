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

package io.crate.expression;

import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.crate.core.collections.StringObjectMaps.fromMapByPath;

public final class ValueExtractors {

    public static Object stringAsBytesRef(Object o) {
        return o instanceof String ? new BytesRef((String) o) : o;
    }

    public static Function<Map<String, Object>, Object> fromMap(ColumnIdent column) {
        if (column.isTopLevel()) {
            return new GetFromMap(column.name());
        } else {
            return new GetFromMapNested(column);
        }
    }

    public static Function<Row, Object> fromRow(int idx, List<String> subscript) {
        return new FromRowWithSubscript(idx, subscript);
    }

    private static class GetFromMap implements Function<Map<String, Object>, Object> {
        private final String name;

        GetFromMap(String name) {
            this.name = name;
        }

        @Override
        public Object apply(Map<String, Object> map) {
            return stringAsBytesRef(map.get(name));
        }
    }

    private static class GetFromMapNested implements Function<Map<String, Object>, Object> {
        private final ColumnIdent column;

        GetFromMapNested(ColumnIdent column) {
            this.column = column;
        }

        @Override
        public Object apply(Map<String, Object> map) {
            Object m = map.get(column.name());
            if (m instanceof Map) {
                return stringAsBytesRef(fromMapByPath((Map) m, column.path()));
            }
            return null;
        }
    }

    private static class FromRowWithSubscript implements Function<Row, Object> {
        private final int idx;
        private final List<String> subscript;

        FromRowWithSubscript(int idx, List<String> subscript) {
            this.idx = idx;
            this.subscript = subscript;
        }

        @Override
        public Object apply(Row row) {
            Object o = row.get(idx);
            if (o instanceof Map) {
                return stringAsBytesRef(fromMapByPath((Map) o, subscript));
            }
            return null;
        }
    }
}
