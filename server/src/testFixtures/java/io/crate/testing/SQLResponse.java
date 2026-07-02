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

package io.crate.testing;

import java.util.Arrays;

import org.jspecify.annotations.Nullable;

import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public record SQLResponse(String[] cols, Object[][] rows, DataType<?>[] columnTypes, long rowCount) {

    public SQLResponse {
        assert cols.length == columnTypes.length : "cols and colTypes differ";
        // Convert byte->short to normalize result between Session/jdbc. In PGTypes byte is mapped to int2.
        // Can't convert the PG version using `executeAndConvertResult` / `ResultSetParser`
        // because from ResultSetMetadata alone it isn't known if the type was byte/int2.
        for (int i = 0; i < columnTypes.length; i++) {
            for (Object[] row : rows) {
                row[i] = normalizeByteValue(row[i], columnTypes[i]);
            }
        }
    }

    private static Object normalizeByteValue(Object val, DataType<?> type) {
        if (val == null) {
            return null;
        }

        var normalizedType = normalizeType(type);
        if (type.equals(normalizedType)) {
            return val;
        }

        return normalizedType.implicitCast(val);
    }

    private static DataType<?> normalizeType(DataType<?> type) {
        int dimensions = ArrayType.dimensions(type);
        DataType<?> innerType = ArrayType.unnest(type);
        if (innerType.id() == DataTypes.BYTE.id()) {
            return ArrayType.makeArray(DataTypes.SHORT, dimensions);
        }
        return type;
    }

    private static String arrayToString(@Nullable Object[] array) {
        return array == null ? null : Arrays.toString(array);
    }

    @Override
    public String toString() {
        return "SQLResponse{" +
                "cols=" + arrayToString(cols()) +
                "colTypes=" + arrayToString(columnTypes()) +
                ", rows=" + ((rows != null) ? rows.length : -1) +
                ", rowCount=" + rowCount +
                '}';
    }
}
