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
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.StreamBucket;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FetchResponse extends ActionResponse {
    private Bucket rows;
    private boolean isLast;
    private Collection<? extends DataType> columnTypes;

    public FetchResponse() {}

    public FetchResponse(Bucket rows, boolean isLast, Collection<? extends DataType> columnTypes) {
        this.rows = rows;
        this.isLast = isLast;
        this.columnTypes = columnTypes;
    }

    public Bucket rows() {
        return rows;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(isLast);
        out.writeVInt(rows.size());

        if (rows.size() > 0) {
            out.writeVInt(columnTypes.size());
            for (DataType columnType : columnTypes) {
                DataTypes.toStream(columnType, out);
            }
            StreamBucket.writeBucket(out, DataTypes.getStreamer(columnTypes), rows);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        isLast = in.readBoolean();
        int numRows = in.readVInt();
        if (numRows > 0) {
            int numCols = in.readVInt();
            List<DataType> columnTypes = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                columnTypes.add(DataTypes.fromStream(in));
            }
            this.columnTypes = columnTypes;
            StreamBucket streamBucket = new StreamBucket(DataTypes.getStreamer(columnTypes));
            streamBucket.readFrom(in);
            rows = streamBucket;
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
