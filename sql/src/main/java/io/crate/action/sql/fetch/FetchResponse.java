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

package io.crate.action.sql.fetch;

import io.crate.action.sql.FetchProperties;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FetchResponse extends ActionResponse {
    private Object[][] rows;
    private boolean isLast;

    public FetchResponse() {}

    public FetchResponse(Object[][] rows, boolean isLast) {
        this.rows = rows;
        this.isLast = isLast;
    }

    public Object[][] rows() {
        return rows;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(isLast);
        out.writeVInt(rows.length);

        if (rows.length > 0) {
            out.writeVInt(rows[0].length); // all rows have the same number of columns, so only write this once
            for (Object[] row : rows) {
                for (Object o : row) {
                    out.writeGenericValue(o);
                }
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        isLast = in.readBoolean();
        int numRows = in.readVInt();
        rows = new Object[numRows][];
        if (numRows > 0) {
            int numCols = in.readVInt();
            for (int r = 0; r < numRows; r++) {
                rows[r] = new Object[numCols];
                for (int c = 0; c < numCols; c++) {
                    rows[r][c] = in.readGenericValue();
                }
            }
        }
    }

    /**
     * Indicates if more FetchRequests can be made.
     * If {@link FetchProperties#closeContext()} was true, isLast will always return true
     *
     * @return false if more data is available, otherwise true
     */
    public boolean isLast() {
        return isLast;
    }
}
