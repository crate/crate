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

import io.crate.types.*;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Arrays;

public class SQLResponse extends ActionResponse implements ToXContent, SQLResult {

    static final class Fields {
        static final XContentBuilderString COLS = new XContentBuilderString("cols");
        static final XContentBuilderString COLUMNTYPES = new XContentBuilderString("colTypes");
        static final XContentBuilderString ROWS = new XContentBuilderString("rows");
        static final XContentBuilderString ROWCOUNT = new XContentBuilderString("rowcount");
        static final XContentBuilderString DURATION = new XContentBuilderString("duration");
    }
    public static final long NO_ROW_COUNT = -1L;

    private Object[][] rows;
    private String[] cols;
    private long rowCount = NO_ROW_COUNT;
    private long requestStartedTime = 0L;
    private DataType[] colTypes;
    private boolean includeTypes = false;
    private final static DataType[] EMPTY_TYPES = new DataType[0];

    public SQLResponse() {
    }

    public SQLResponse(String[] cols, Object[][] rows, long rowCount, long requestStartedTime) {
        this(cols, rows, new DataType[0], rowCount, requestStartedTime, false);
    }

    public SQLResponse(String[] cols,
                       Object[][] rows,
                       DataType[] dataTypes,
                       long rowCount,
                       long requestStartedTime,
                       boolean includeTypes) {
        this.cols = cols;
        this.rows = rows;
        this.rowCount = rowCount;
        this.requestStartedTime = requestStartedTime;
        this.includeTypes = includeTypes;

        // output types could list of one LongType, used internally for affectedRows of DML nodes
        // remove it here, create empty array
        if (cols.length == 0 && dataTypes.length == 1 && dataTypes[0].equals(LongType.INSTANCE)) {
            this.colTypes = EMPTY_TYPES;
        } else {
            this.colTypes = dataTypes;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(Fields.COLS, cols);
        if (includeTypes) {
            builder.startArray(Fields.COLUMNTYPES);
            if (colTypes != null) {
                for (int i = 0; i < colTypes.length; i++) {
                    toXContentNestedDataType(builder, colTypes[i]);
                }
            }
            builder.endArray();
        }
        builder.startArray(Fields.ROWS);
        if (rows != null) {
            for (int i = 0; i < rows.length; i++) {
                builder.startArray();
                for (int j = 0; j < cols.length; j++) {
                    builder.value(rows[i][j]);
                }
                builder.endArray();
            }
        }
        builder.endArray();
        builder.field(Fields.ROWCOUNT, rowCount());
        builder.field(Fields.DURATION, duration());

        return builder;
    }

    public static void toXContentNestedDataType(XContentBuilder builder, DataType dataType) throws IOException {
        if (dataType instanceof CollectionType) {
            builder.startArray();
            builder.value(dataType.id());
            toXContentNestedDataType(builder, ((CollectionType) dataType).innerType());
            builder.endArray();
        } else {
            builder.value(dataType.id());
        }
    }

    public String[] cols(){
        return cols;
    }

    public void cols(String[] cols){
        this.cols = cols;
    }

    public DataType[] columnTypes() {
        return colTypes;
    }

    public void colTypes(DataType[] dataTypes) {
        colTypes = dataTypes;
    }

    public void includeTypes(boolean includeTypes) {
        this.includeTypes = includeTypes;
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

    public long duration() {
        if (requestStartedTime > 0) {
            return System.currentTimeMillis()- requestStartedTime;
        }
        return -1;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        boolean negative = in.readBoolean();
        rowCount = in.readVLong();
        if (negative) {
            rowCount = -rowCount;
        }
        cols = in.readStringArray();
        int numRows = in.readInt();
        rows = new Object[numRows][cols.length];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < cols.length; j++) {
                rows[i][j] = in.readGenericValue();
            }
        }
        requestStartedTime = in.readVLong();
        includeTypes = in.readBoolean();
        if (includeTypes) {
            int numColumnTypes = in.readInt();
            colTypes = new DataType[numColumnTypes];
            for (int i = 0; i < numColumnTypes; i++) {
                colTypes[i] = DataTypes.fromStream(in);
            }
        } else {
            colTypes = EMPTY_TYPES;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(rowCount < 0);
        out.writeVLong(Math.abs(rowCount));
        out.writeStringArray(cols);
        out.writeInt(rows.length);
        for (int i = 0; i < rows.length ; i++) {
            for (int j = 0; j < cols.length; j++) {
                out.writeGenericValue(rows[i][j]);
            }
        }
        out.writeVLong(requestStartedTime);
        out.writeBoolean(includeTypes);
        if (includeTypes) {
            out.writeInt(colTypes.length);
            for (int i = 0; i < colTypes.length; i++) {
                DataTypes.toStream(colTypes[i], out);
            }
        }
    }

    @Override
    public String toString() {
        return "SQLResponse{" +
                "cols=" + ((cols!=null) ? Arrays.toString(cols): null) +
                "colTypes=" + ((colTypes !=null) ? Arrays.toString(colTypes): null) +
                ", rows=" + ((rows!=null) ? rows.length: -1)  +
                ", rowCount=" + rowCount  +
                ", duration=" + duration()  +
                '}';
    }
}
