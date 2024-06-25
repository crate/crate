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

package io.crate.rest.action;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

class ResultToXContentBuilder {


    static final class FIELDS {
        static final String RESULTS = "results";
        static final String COLS = "cols";
        static final String COLUMN_TYPES = "col_types";
        static final String ROWS = "rows";
        static final String ROW_COUNT = "rowcount";
        static final String DURATION = "duration";
        static final String ERROR_MESSAGE = "error_message";
    }

    private final XContentBuilder builder;

    private ResultToXContentBuilder(XContentBuilder builder) throws IOException {
        this.builder = builder;
        builder.startObject();
    }

    static ResultToXContentBuilder builder(XContentBuilder builder) throws IOException {
        return new ResultToXContentBuilder(builder);
    }

    ResultToXContentBuilder cols(List<? extends Symbol> fields) throws IOException {
        builder.startArray(FIELDS.COLS);
        for (Symbol field : fields) {
            builder.value(field.toColumn().sqlFqn());
        }
        builder.endArray();
        return this;
    }

    ResultToXContentBuilder colTypes(List<? extends Symbol> fields) throws IOException {
        builder.startArray(FIELDS.COLUMN_TYPES);
        for (Symbol field : fields) {
            toXContentNestedDataType(builder, field.valueType());
        }
        builder.endArray();
        return this;
    }

    private void toXContentNestedDataType(XContentBuilder builder, DataType dataType) throws IOException {
        if (dataType instanceof ArrayType) {
            builder.startArray();
            builder.value(dataType.id());
            toXContentNestedDataType(builder, ((ArrayType) dataType).innerType());
            builder.endArray();
        } else {
            builder.value(dataType.id());
        }
    }

    ResultToXContentBuilder duration(long startTime) throws IOException {
        builder.field(FIELDS.DURATION, (float) ((System.nanoTime() - startTime) / 1_000_000.0));
        return this;
    }

    /**
     * startRows() must be called before first setNextRow()
     */
    ResultToXContentBuilder startRows() throws IOException {
        builder.startArray(FIELDS.ROWS);
        return this;
    }

    /**
     * finishRows() must be called after last setNextRow()
     */
    ResultToXContentBuilder finishRows() throws IOException {
        builder.endArray();
        return this;
    }

    /**
     * addRow() should be called from setNextRow()
     * make sure startRows() is called before and finishRows() is called thereafter
     * @param row
     * @param numCols
     */
    ResultToXContentBuilder addRow(Row row, int numCols) throws IOException {
        builder.startArray();
        for (int j = 0; j < numCols; j++) {
            builder.value(row.get(j));
        }
        builder.endArray();
        return this;
    }

    /**
     * rowCount() can override the internal row counter that is increased upon every addRow() call
     * @param rowCount
     */
    ResultToXContentBuilder rowCount(long rowCount) throws IOException {
        builder.field(FIELDS.ROW_COUNT, rowCount);
        return this;
    }

    ResultToXContentBuilder bulkRows(RestBulkRowCountReceiver.Result[] results) throws IOException {
        builder.startArray(FIELDS.RESULTS);
        for (RestBulkRowCountReceiver.Result result : results) {
            builder.startObject();
            builder.field(FIELDS.ROW_COUNT, result.rowCount());
            if (result.errorMessage() != null) {
                builder.field(FIELDS.ERROR_MESSAGE, result.errorMessage());
            }
            builder.endObject();
        }
        builder.endArray();
        return this;
    }

    XContentBuilder build() throws IOException {
        builder.endObject();
        return builder;
    }
}
