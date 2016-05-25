/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public abstract class SQLBaseResponse extends ActionResponse implements ToXContent {

    public static final DataType[] EMPTY_TYPES = new DataType[0];

    static final class Fields {
        static final XContentBuilderString RESULTS = new XContentBuilderString("results");
        static final XContentBuilderString COLS = new XContentBuilderString("cols");
        static final XContentBuilderString COLUMNTYPES = new XContentBuilderString("colTypes");
        static final XContentBuilderString ROWS = new XContentBuilderString("rows");
        static final XContentBuilderString ROWCOUNT = new XContentBuilderString("rowcount");
        static final XContentBuilderString DURATION = new XContentBuilderString("duration");
        static final XContentBuilderString ERROR_MESSAGE = new XContentBuilderString("error_message");
    }

    private String[] cols;
    private DataType[] colTypes;
    private boolean includeTypes;
    private float duration;

    public SQLBaseResponse() {} // used for serialization

    public SQLBaseResponse(String[] cols, DataType[] colTypes, boolean includeTypes, float duration) {
        assert cols.length == colTypes.length : "cols and colTypes differ";
        this.cols = cols;
        this.colTypes = colTypes;
        this.includeTypes = includeTypes;
        this.duration = duration;
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
        this.colTypes = dataTypes;
    }

    public void includeTypes(boolean includeTypes) {
        this.includeTypes = includeTypes;
    }

    public float duration() {
        return duration;
    }

    protected void writeSharedAttributes(XContentBuilder builder) throws IOException {
        builder.array(Fields.COLS, cols);
        if (includeTypes) {
            builder.startArray(Fields.COLUMNTYPES);
            if (colTypes != null) {
                for (DataType colType : colTypes) {
                    toXContentNestedDataType(builder, colType);
                }
            }
            builder.endArray();
        }

        builder.field(Fields.DURATION, duration());
    }

    private static void toXContentNestedDataType(XContentBuilder builder, DataType dataType) throws IOException {
        if (dataType instanceof CollectionType) {
            builder.startArray();
            builder.value(dataType.id());
            toXContentNestedDataType(builder, ((CollectionType) dataType).innerType());
            builder.endArray();
        } else {
            builder.value(dataType.id());
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cols = in.readStringArray();
        includeTypes = in.readBoolean();
        duration = in.readFloat();
        if (includeTypes) {
            int numColTypes = in.readVInt();
            colTypes = new DataType[numColTypes];
            for (int i = 0; i < numColTypes; i++) {
                colTypes[i] = DataTypes.fromStream(in);
            }
        } else {
            colTypes = EMPTY_TYPES;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(cols);
        out.writeBoolean(includeTypes);
        out.writeFloat(duration);
        if (includeTypes) {
            out.writeVInt(colTypes.length);
            for (DataType colType : colTypes) {
                DataTypes.toStream(colType, out);
            }
        }
    }
}
