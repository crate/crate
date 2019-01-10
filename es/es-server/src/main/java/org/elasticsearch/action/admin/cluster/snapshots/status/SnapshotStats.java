/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;

public class SnapshotStats implements Streamable, ToXContentObject {

    private long startTime;
    private long time;
    private int incrementalFileCount;
    private int totalFileCount;
    private int processedFileCount;
    private long incrementalSize;
    private long totalSize;
    private long processedSize;

    SnapshotStats() {
    }

    SnapshotStats(long startTime, long time,
                  int incrementalFileCount, int totalFileCount, int processedFileCount,
                  long incrementalSize, long totalSize, long processedSize) {
        this.startTime = startTime;
        this.time = time;
        this.incrementalFileCount = incrementalFileCount;
        this.totalFileCount = totalFileCount;
        this.processedFileCount = processedFileCount;
        this.incrementalSize = incrementalSize;
        this.totalSize = totalSize;
        this.processedSize = processedSize;
    }

    /**
     * Returns time when snapshot started
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns snapshot running time
     */
    public long getTime() {
        return time;
    }

    /**
     * Returns incremental file count of the snapshot
     */
    public int getIncrementalFileCount() {
        return incrementalFileCount;
    }

    /**
     * Returns total number of files in the snapshot
     */
    public int getTotalFileCount() {
        return totalFileCount;
    }

    /**
     * Returns number of files in the snapshot that were processed so far
     */
    public int getProcessedFileCount() {
        return processedFileCount;
    }

    /**
     * Return incremental files size of the snapshot
     */
    public long getIncrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns total size of files in the snapshot
     */
    public long getTotalSize() {
        return totalSize;
    }

    /**
     * Returns total size of files in the snapshot that were processed so far
     */
    public long getProcessedSize() {
        return processedSize;
    }


    public static SnapshotStats readSnapshotStats(StreamInput in) throws IOException {
        SnapshotStats stats = new SnapshotStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startTime);
        out.writeVLong(time);

        out.writeVInt(incrementalFileCount);
        out.writeVInt(processedFileCount);

        out.writeVLong(incrementalSize);
        out.writeVLong(processedSize);

        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeVInt(totalFileCount);
            out.writeVLong(totalSize);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        startTime = in.readVLong();
        time = in.readVLong();

        incrementalFileCount = in.readVInt();
        processedFileCount = in.readVInt();

        incrementalSize = in.readVLong();
        processedSize = in.readVLong();

        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            totalFileCount = in.readVInt();
            totalSize = in.readVLong();
        } else {
            totalFileCount = incrementalFileCount;
            totalSize = incrementalSize;
        }
    }

    static final class Fields {
        static final String STATS = "stats";

        static final String INCREMENTAL = "incremental";
        static final String PROCESSED = "processed";
        static final String TOTAL = "total";

        static final String FILE_COUNT = "file_count";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";

        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String TIME = "time";

        // BWC
        static final String NUMBER_OF_FILES = "number_of_files";
        static final String PROCESSED_FILES = "processed_files";
        static final String TOTAL_SIZE = "total_size";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
        static final String PROCESSED_SIZE_IN_BYTES = "processed_size_in_bytes";
        static final String PROCESSED_SIZE = "processed_size";

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(Fields.INCREMENTAL);
            {
                builder.field(Fields.FILE_COUNT, getIncrementalFileCount());
                builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getIncrementalSize()));
            }
            builder.endObject();

            if (getProcessedFileCount() != getIncrementalFileCount()) {
                builder.startObject(Fields.PROCESSED);
                {
                    builder.field(Fields.FILE_COUNT, getProcessedFileCount());
                    builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getProcessedSize()));
                }
                builder.endObject();
            }

            builder.startObject(Fields.TOTAL);
            {
                builder.field(Fields.FILE_COUNT, getTotalFileCount());
                builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getTotalSize()));
            }
            builder.endObject();

            // timings stats
            builder.field(Fields.START_TIME_IN_MILLIS, getStartTime());
            builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(getTime()));

            // BWC part
            builder.field(Fields.NUMBER_OF_FILES, getIncrementalFileCount());
            builder.field(Fields.PROCESSED_FILES, getProcessedFileCount());
            builder.humanReadableField(Fields.TOTAL_SIZE_IN_BYTES, Fields.TOTAL_SIZE, new ByteSizeValue(getIncrementalSize()));
            builder.humanReadableField(Fields.PROCESSED_SIZE_IN_BYTES, Fields.PROCESSED_SIZE, new ByteSizeValue(getProcessedSize()));
            // BWC part ends
        }
        return builder.endObject();
    }

    public static SnapshotStats fromXContent(XContentParser parser) throws IOException {
        // Parse this old school style instead of using the ObjectParser since there's an impedance mismatch between how the
        // object has historically been written as JSON versus how it is structured in Java.
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        int totalFileCount = 0;
        int processedFileCount = 0;
        long incrementalSize = 0;
        long totalSize = 0;
        long processedSize = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String currentName = parser.currentName();
            token = parser.nextToken();
            if (currentName.equals(Fields.INCREMENTAL)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                    String innerName = parser.currentName();
                    token = parser.nextToken();
                    if (innerName.equals(Fields.FILE_COUNT)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        incrementalFileCount = parser.intValue();
                    } else if (innerName.equals(Fields.SIZE_IN_BYTES)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        incrementalSize = parser.longValue();
                    } else {
                        // Unknown sub field, skip
                        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            } else if (currentName.equals(Fields.PROCESSED)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                    String innerName = parser.currentName();
                    token = parser.nextToken();
                    if (innerName.equals(Fields.FILE_COUNT)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        processedFileCount = parser.intValue();
                    } else if (innerName.equals(Fields.SIZE_IN_BYTES)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        processedSize = parser.longValue();
                    } else {
                        // Unknown sub field, skip
                        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            } else if (currentName.equals(Fields.TOTAL)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                    String innerName = parser.currentName();
                    token = parser.nextToken();
                    if (innerName.equals(Fields.FILE_COUNT)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        totalFileCount = parser.intValue();
                    } else if (innerName.equals(Fields.SIZE_IN_BYTES)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        totalSize = parser.longValue();
                    } else {
                        // Unknown sub field, skip
                        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            } else if (currentName.equals(Fields.START_TIME_IN_MILLIS)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                startTime = parser.longValue();
            } else if (currentName.equals(Fields.TIME_IN_MILLIS)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
                time = parser.longValue();
            } else {
                // Unknown field, skip
                if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        return new SnapshotStats(startTime, time, incrementalFileCount, totalFileCount, processedFileCount, incrementalSize, totalSize,
            processedSize);
    }

    void add(SnapshotStats stats) {
        incrementalFileCount += stats.incrementalFileCount;
        totalFileCount += stats.totalFileCount;
        processedFileCount += stats.processedFileCount;

        incrementalSize += stats.incrementalSize;
        totalSize += stats.totalSize;
        processedSize += stats.processedSize;

        if (startTime == 0) {
            // First time here
            startTime = stats.startTime;
            time = stats.time;
        } else {
            // The time the last snapshot ends
            long endTime = Math.max(startTime + time, stats.startTime + stats.time);

            // The time the first snapshot starts
            startTime = Math.min(startTime, stats.startTime);

            // Update duration
            time = endTime - startTime;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotStats that = (SnapshotStats) o;

        if (startTime != that.startTime) return false;
        if (time != that.time) return false;
        if (incrementalFileCount != that.incrementalFileCount) return false;
        if (totalFileCount != that.totalFileCount) return false;
        if (processedFileCount != that.processedFileCount) return false;
        if (incrementalSize != that.incrementalSize) return false;
        if (totalSize != that.totalSize) return false;
        return processedSize == that.processedSize;
    }

    @Override
    public int hashCode() {
        int result = (int) (startTime ^ (startTime >>> 32));
        result = 31 * result + (int) (time ^ (time >>> 32));
        result = 31 * result + incrementalFileCount;
        result = 31 * result + totalFileCount;
        result = 31 * result + processedFileCount;
        result = 31 * result + (int) (incrementalSize ^ (incrementalSize >>> 32));
        result = 31 * result + (int) (totalSize ^ (totalSize >>> 32));
        result = 31 * result + (int) (processedSize ^ (processedSize >>> 32));
        return result;
    }
}
