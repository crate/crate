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

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.executor.BytesRefUtils;
import io.crate.executor.transport.StreamBucket;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class SQLResponse extends SQLBaseResponse {

    public static final long NO_ROW_COUNT = -1L;

    private Bucket bucket;
    private long rowCount = NO_ROW_COUNT;
    private UUID cursorId;
    private Object[][] rows;

    public SQLResponse() {
    }

    public SQLResponse(String[] cols,
                       Bucket bucket,
                       DataType[] dataTypes,
                       long rowCount,
                       float duration,
                       UUID cursorId) {
        super(cols, dataTypes, true, duration);
        this.bucket = bucket;
        this.rowCount = rowCount;
        this.cursorId = cursorId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        writeSharedAttributes(builder);
        builder.startArray(Fields.ROWS);
        for (Row row : bucket) {
            builder.startArray();
            for (int j = 0, len = cols().length; j < len; j++) {
                builder.value(row.get(j));
            }
            builder.endArray();
        }
        builder.endArray();
        builder.field(Fields.ROWCOUNT, rowCount());
        builder.endObject();
        return builder;
    }

    /**
     * @deprecated rows() will be removed in X.XX - use {@link #bucket()} instead
     */
    @Deprecated
    public Object[][] rows() {
        if (rows == null) {
            rows = Buckets.materialize(bucket);
            BytesRefUtils.ensureStringTypesAreStrings(colTypes, rows);
        }
        return rows;
    }

    public Bucket bucket() {
        return bucket;
    }

    public UUID cursorId() {
        return cursorId;
    }

    public long rowCount() {
        return rowCount;
    }

    public boolean hasRowCount() {
        return this.rowCount() > NO_ROW_COUNT;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        cursorId = new UUID(in.readLong(), in.readLong());
        boolean negative = in.readBoolean();
        rowCount = in.readVLong();
        if (negative) {
            rowCount = -rowCount;
        }

        StreamBucket streamBucket = new StreamBucket(streamers());
        streamBucket.readFrom(in);
        bucket = streamBucket;
    }

    private Streamer<?>[] streamers() {
        DataType[] dataTypes = columnTypes();
        Streamer[] streamers = new Streamer[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            streamers[i] = dataTypes[i].streamer();
        }
        return streamers;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeLong(cursorId.getMostSignificantBits());
        out.writeLong(cursorId.getLeastSignificantBits());
        out.writeBoolean(rowCount < 0);
        out.writeVLong(Math.abs(rowCount));
        StreamBucket.writeBucket(out, streamers(), bucket);
    }

    private static String arrayToString(@Nullable Object[] array) {
        return array == null ? null : Arrays.toString(array);
    }

    @Override
    public String toString() {
        return "SQLResponse{" +
                "cols=" + arrayToString(cols()) +
                "colTypes=" + arrayToString(columnTypes()) +
                ", rows=" + (bucket == null ? -1 : bucket.size()) +
                ", rowCount=" + rowCount  +
                ", duration=" + duration()  +
                '}';
    }
}
