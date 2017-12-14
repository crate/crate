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

package io.crate.testing;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.ImmutableSet;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BytesRefUtils {

    private final static Set<DataType> BYTES_REF_TYPES = ImmutableSet.of(DataTypes.STRING, DataTypes.IP);

    public static void ensureStringTypesAreStrings(DataType[] dataTypes, Object[][] rows) {
        if (rows.length == 0) {
            return;
        }

        final IntArrayList stringColumns = new IntArrayList();
        final IntArrayList stringCollectionColumns = new IntArrayList();
        final IntArrayList objectColumns = new IntArrayList();
        final IntArrayList objectCollectionColumns = new IntArrayList();
        int idx = 0;
        for (DataType dataType : dataTypes) {
            if (BYTES_REF_TYPES.contains(dataType)) {
                stringColumns.add(idx);
            } else if ((DataTypes.isCollectionType(dataType))) {
                DataType<?> innerType = ((CollectionType) dataType).innerType();
                if (BYTES_REF_TYPES.contains(innerType)) {
                    stringCollectionColumns.add(idx);
                } else if (DataTypes.OBJECT.equals(innerType)) {
                    objectCollectionColumns.add(idx);
                }
            } else if (DataTypes.OBJECT.equals(dataType)) {
                objectColumns.add(idx);
            }
            idx++;
        }

        for (Object[] row : rows) {
            convertStringColumns(row, stringColumns);
            convertStringCollectionColumns(row, stringCollectionColumns);
            convertObjectColumns(row, objectColumns);
            convertObjectCollectionColumns(row, objectCollectionColumns);
        }
    }

    private static void convertStringCollectionColumns(Object[] row, IntArrayList stringCollectionColumns) {
        for (IntCursor stringCollectionColumn : stringCollectionColumns) {
            Object value = row[stringCollectionColumn.value];
            if (value == null) {
                continue;
            }
            if (value instanceof Set) {
                row[stringCollectionColumn.value] = setToStringArray(((Set<BytesRef>) value));
            } else if (value instanceof Object[]) {
                row[stringCollectionColumn.value] = ensureStringValuesInArray((Object[]) value);
            }
        }
    }

    private static void convertStringColumns(Object[] row, IntArrayList stringColumns) {
        for (IntCursor stringColumn : stringColumns) {
            Object value = row[stringColumn.value];
            if (value instanceof BytesRef) {
                row[stringColumn.value] = ((BytesRef) value).utf8ToString();
            } else if (value instanceof String) {
                throw new IllegalArgumentException("Column: " + stringColumn.index + " cannot be of type String, " +
                                                   "BytesRef should be used instead");
            }
        }
    }

    private static void convertObjectColumns(Object[] row, IntArrayList columns) {
        for (IntCursor indexRef : columns) {
            Object value = row[indexRef.value];
            if (value == null) {
                continue;
            }
            if (value instanceof Map) {
                row[indexRef.value] = ensureStringValuesInMap((Map<String, Object>) value);
            }
        }
    }

    private static void convertObjectCollectionColumns(Object[] row, IntArrayList columns) {
        for (IntCursor indexRef : columns) {
            Object value = row[indexRef.value];
            if (value == null) {
                continue;
            }
            if (value instanceof Object[]) {
                Object[] objectArr = (Object[]) value;
                for (int i = 0; i < objectArr.length; i++) {
                    objectArr[i] = ensureStringValuesInMap((Map<String, Object>) objectArr[i]);
                }
            }
        }
    }

    private static String[] setToStringArray(Set<BytesRef> values) {
        String[] strings = new String[values.size()];
        int idx = 0;
        for (BytesRef value : values) {
            strings[idx] = value == null ? null : value.utf8ToString();
            idx++;
        }
        return strings;
    }

    private static Object[] ensureStringValuesInArray(Object[] values) {
        if (values.length > 0) {
            int idx = 0;
            Object firstNotNull = values[idx];
            while (firstNotNull == null && idx < values.length) {
                firstNotNull = values[idx++];
            }
            if (firstNotNull == null) {
                return values;
            } else if (firstNotNull instanceof BytesRef) {
                // Column is ARRAY(STRING)
                // we want to be able to cast the value in the sql response to String[]
                String[] converted = new String[values.length];
                for (int i = 0; i < converted.length; i++) {
                    converted[i] = BytesRefs.toString(values[i]);
                }
                return converted;
            } else if (firstNotNull instanceof Map) {
                // Column is ARRAY(OBJECT)
                Map[] converted = new Map[values.length];
                for (int i = 0; i < converted.length; i++) {
                    converted[i] = ensureStringValuesInMap((Map<String, Object>) values[i]);
                }
                return converted;
            } else if (firstNotNull instanceof String) {
                throw new IllegalArgumentException("Column: " + idx + " cannot be of type String, " +
                                                   "BytesRef should be used instead");
            }
            return values;
        }
        return new Object[0];
    }

    private static Map<String, Object> ensureStringValuesInMap(Map<String, Object> value) {
        HashMap<String, Object> mapCopy = new HashMap<>(value);
        for (Map.Entry<String, Object> entry : mapCopy.entrySet()) {
            Object innerValue = entry.getValue();
            if (innerValue == null) {
                continue;
            }
            if (innerValue instanceof BytesRef) {
                entry.setValue(BytesRefs.toString(innerValue));
            } else if (innerValue instanceof Object[]) {
                entry.setValue(ensureStringValuesInArray((Object[]) innerValue));
            } else if (innerValue instanceof Map) {
                entry.setValue(ensureStringValuesInMap((Map<String, Object>) innerValue));
            }
        }
        return mapCopy;
    }
}
