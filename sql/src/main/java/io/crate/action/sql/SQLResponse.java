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

package io.crate.action.sql;

import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

public class SQLResponse extends SQLBaseResponse {

    public static final long NO_ROW_COUNT = -1L;

    private Object[][] rows;
    private long rowCount = NO_ROW_COUNT;

    public SQLResponse() {
    }

    public SQLResponse(String[] cols,
                       Object[][] rows,
                       DataType[] dataTypes,
                       long rowCount,
                       float duration,
                       boolean includeTypes) {
        super(cols, dataTypes, includeTypes, duration);
        this.rows = rows;
        this.rowCount = rowCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        writeSharedAttributes(builder);
        builder.startArray(Fields.ROWS);
        if (rows != null) {
            for (Object[] row : rows) {
                builder.startArray();
                for (int j = 0, len = cols().length; j < len; j++) {
                    builder.value(row[j]);
                }
                builder.endArray();
            }
        }
        builder.endArray();
        builder.field(Fields.ROWCOUNT, rowCount());
        builder.endObject();
        return builder;
    }

    public Object[][] rows(){
        return rows;
    }

    public long rowCount() {
        return rowCount;
    }

    public void rowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public boolean hasRowCount() {
        return this.rowCount() > NO_ROW_COUNT;
    }

    public void rows(Object[][] rows) {
        this.rows = rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        boolean negative = in.readBoolean();
        rowCount = in.readVLong();
        if (negative) {
            rowCount = -rowCount;
        }

        int numCols = cols().length;
        int numRows = in.readInt();
        rows = new Object[numRows][numCols];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                rows[i][j] = in.readGenericValue();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeBoolean(rowCount < 0);
        out.writeVLong(Math.abs(rowCount));
        out.writeInt(rows.length);
        for (Object[] row : rows) {
            for (int j = 0, len = cols().length; j < len; j++) {
                out.writeGenericValue(row[j]);
            }
        }
    }

    private static String arrayToString(@Nullable Object[] array) {
        return array == null ? null : Arrays.toString(array);
    }

    @Override
    public String toString() {
        return "SQLResponse{" +
                "cols=" + arrayToString(cols()) +
                "colTypes=" + arrayToString(columnTypes()) +
                ", rows=" + ((rows!=null) ? rows.length: -1)  +
                ", rowCount=" + rowCount  +
                ", duration=" + duration()  +
                '}';
    }
}
