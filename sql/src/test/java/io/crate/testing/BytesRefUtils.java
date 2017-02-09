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

import java.util.Set;

public class BytesRefUtils {

    private final static Set<DataType> BYTES_REF_TYPES = ImmutableSet.<DataType>of(DataTypes.STRING, DataTypes.IP);

    public static void ensureStringTypesAreStrings(DataType[] dataTypes, Object[][] rows) {
        if (rows.length == 0) {
            return;
        }

        // NOTE: currently BytesRef inside Maps aren't converted here because
        // if the map is coming from a ESSearchTask/EsGetTask they already contain strings
        // and we have no case in which another Task returns a Map with ByteRefs/Strings inside.
        final IntArrayList stringColumns = new IntArrayList();
        final IntArrayList stringCollectionColumns = new IntArrayList();
        int idx = 0;
        for (DataType dataType : dataTypes) {
            if (BYTES_REF_TYPES.contains(dataType)) {
                stringColumns.add(idx);
            } else if ((DataTypes.isCollectionType(dataType)
                        && (BYTES_REF_TYPES.contains(((CollectionType) dataType).innerType())))) {
                stringCollectionColumns.add(idx);
            }
            idx++;
        }

        for (Object[] row : rows) {
            convertStringColumns(row, stringColumns);
            convertStringCollectionColumns(row, stringCollectionColumns);
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
            } else if (value instanceof BytesRef[]) {
                row[stringCollectionColumn.value] = objectArrayToStringArray(((BytesRef[]) value));
            } else if (value instanceof Object[]) {
                row[stringCollectionColumn.value] = objectArrayToStringArray(((Object[]) value));
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

    private static String[] objectArrayToStringArray(Object[] values) {
        String[] strings = new String[values.length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = BytesRefs.toString(values[i]);
        }
        return strings;
    }

    private static void convertStringColumns(Object[] row, IntArrayList stringColumns) {
        for (IntCursor stringColumn : stringColumns) {
            Object value = row[stringColumn.value];
            if (value instanceof BytesRef) {
                row[stringColumn.value] = ((BytesRef) value).utf8ToString();
            }
        }
    }
}
