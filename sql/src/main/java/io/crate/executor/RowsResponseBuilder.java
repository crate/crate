/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.SQLResponse;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class RowsResponseBuilder implements ResponseBuilder {

    /**
     * TODO: let the client set this flag through SQLRequest and Planner
     */
    private final boolean convertBytesRefs;

    private final static Set<DataType> BYTES_REF_TYPES = ImmutableSet.<DataType>of(
            DataTypes.STRING, DataTypes.IP);

    public RowsResponseBuilder(boolean convertBytesRefs) {
        this.convertBytesRefs = convertBytesRefs;
    }

    @Override
    public SQLResponse buildResponse(DataType[] dataTypes,
                                     String[] outputNames,
                                     Object[][] rows,
                                     long requestStartedTime,
                                     boolean includeTypes) {
        if (convertBytesRefs) {
            convertBytesRef(dataTypes, rows);
        }
        return new SQLResponse(outputNames, rows, dataTypes, rows.length,
                requestStartedTime, includeTypes);
    }

    private void convertBytesRef(DataType[] dataTypes, Object[][] rows) {
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
                    && (BYTES_REF_TYPES.contains(((CollectionType)dataType).innerType())))) {
                stringCollectionColumns.add(idx);
            }
            idx++;
        }

        for (int r = 0; r < rows.length; r++) {
            for (IntCursor stringColumn : stringColumns) {
                Object value = rows[r][stringColumn.value];
                if (value != null && value instanceof BytesRef) {
                    rows[r][stringColumn.value] = ((BytesRef)value).utf8ToString();
                }
            }

            for (IntCursor stringCollectionColumn : stringCollectionColumns) {
                Object value = rows[r][stringCollectionColumn.value];
                if (value != null) {
                    Iterator<BytesRef> iter = null;
                    int size;
                    if (value instanceof Set) {
                        Set<BytesRef> bytesRefSet = ((Set<BytesRef>) value);
                        iter = bytesRefSet.iterator();
                        size = bytesRefSet.size();
                    } else if (value instanceof BytesRef[]) {
                        BytesRef[] bytesRefArray = (BytesRef[])value;
                        iter = Arrays.asList(bytesRefArray).iterator();
                        size = bytesRefArray.length;
                    } else {
                        return;
                    }

                    String[] valuesString = new String[size];
                    for (int i = 0; i < size; i++) {
                        BytesRef bytesRef = iter.next();
                        valuesString[i] = bytesRef == null ? null : bytesRef.utf8ToString();
                    }
                    rows[r][stringCollectionColumn.value] = valuesString;
                }
            }
        }
    }
}
