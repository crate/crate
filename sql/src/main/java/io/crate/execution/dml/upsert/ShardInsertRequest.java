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

package io.crate.execution.dml.upsert;

import io.crate.Streamer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public class ShardInsertRequest extends AbstractShardWriteRequest<ShardInsertRequest, ShardInsertRequest.Item> {

    private SessionSettings sessionSettings;

    /**
     * List of references used on insert
     */
    private Reference[] insertColumns;

    public ShardInsertRequest(ShardId shardId,
                              UUID jobId,
                              EnumSet<Mode> modes,
                              SessionSettings sessionSettings,
                              Reference[] insertColumns) {
        super(shardId, jobId, modes);
        this.sessionSettings = sessionSettings;
        this.insertColumns = insertColumns;
    }

    public ShardInsertRequest(StreamInput in) throws IOException {
        super(in);
        int missingAssignmentsColumnsSize = in.readVInt();
        Streamer[] insertValuesStreamer = null;
        if (missingAssignmentsColumnsSize > 0) {
            insertColumns = new Reference[missingAssignmentsColumnsSize];
            for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
                insertColumns[i] = Reference.fromStream(in);
            }
            insertValuesStreamer = Symbols.streamerArray(List.of(insertColumns));
        }

        sessionSettings = new SessionSettings(in);
        int numItems = in.readVInt();
        items = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            items.add(new ShardInsertRequest.Item(in, insertValuesStreamer));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Streamer[] insertValuesStreamer = null;
        if (insertColumns != null) {
            out.writeVInt(insertColumns.length);
            for (Reference reference : insertColumns) {
                Reference.toStream(reference, out);
            }
            insertValuesStreamer = Symbols.streamerArray(List.of(insertColumns));
        } else {
            out.writeVInt(0);
        }
        sessionSettings.writeTo(out);

        boolean allOn4_2 = out.getVersion().onOrAfter(Version.V_4_2_0);

        out.writeVInt(items.size());
        for (ShardInsertRequest.Item item : items) {
            item.writeTo(out, insertValuesStreamer, allOn4_2);
        }
    }


    @Nullable
    @Override
    public SessionSettings sessionSettings() {
        return sessionSettings;
    }

    @Nullable
    @Override
    public Symbol[] returnValues() {
        return null;
    }

    @Nullable
    @Override
    public String[] updateColumns() {
        return null;
    }

    @Nullable
    @Override
    public Reference[] insertColumns() {
        return insertColumns;
    }

    /**
     * A single insert item.
     */
    public static class Item extends AbstractShardWriteRequest.Item {

        /**
         * List of objects used on insert
         */
        @Nullable
        private Object[] insertValues;

        public Item(String id,
                    @Nullable Symbol[] updateAssignments,
                    @Nullable Object[] insertValues,
                    @Nullable Long version,
                    @Nullable Long seqNo,
                    @Nullable Long primaryTerm,
                    @Nullable Symbol[] returnValues
        ) {
            super(id, version, seqNo, primaryTerm);
            if (version != null) {
                this.version = version;
            }
            if (seqNo != null) {
                this.seqNo = seqNo;
            }
            if (primaryTerm != null) {
                this.primaryTerm = primaryTerm;
            }
            this.insertValues = insertValues;
        }

        boolean retryOnConflict() {
            return seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && version == Versions.MATCH_ANY;
        }

        @Nullable
        Symbol[] updateAssignments() {
            return null;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
        }

        @Nullable
        public Symbol[] returnValues() {
            return null;
        }

        public Item(StreamInput in, @Nullable Streamer[] insertValueStreamers) throws IOException {
            super(in);
            int missingAssignmentsSize = in.readVInt();
            if (missingAssignmentsSize > 0) {
                assert insertValueStreamers != null : "streamers are required if reading insert values";
                this.insertValues = new Object[missingAssignmentsSize];
                for (int i = 0; i < missingAssignmentsSize; i++) {
                    insertValues[i] = insertValueStreamers[i].readValueFrom(in);
                }
            }
        }

        public void writeTo(StreamOutput out, @Nullable Streamer[] insertValueStreamers, boolean allOn4_2) throws IOException {
            super.writeTo(out);
            if (insertValues != null) {
                assert insertValueStreamers != null : "streamers are required to stream insert values";
                out.writeVInt(insertValues.length);
                for (int i = 0; i < insertValues.length; i++) {
                    insertValueStreamers[i].writeValueTo(out, insertValues[i]);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }


    public static class Builder {

        private final SessionSettings sessionSettings;
        private final TimeValue timeout;
        @Nullable
        private final Reference[] insertColumns;
        private final UUID jobId;
        @Nullable
        private final EnumSet<Mode> modes;

        public Builder(SessionSettings sessionSettings,
                       TimeValue timeout,
                       boolean continueOnError,
                       Reference[] insertColumns,
                       UUID jobId,
                       boolean validateGeneratedColumns,
                       Mode... modes) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.insertColumns = insertColumns;
            this.jobId = jobId;
            this.modes = EnumSet.copyOf(List.of(modes));
            if (validateGeneratedColumns) {
                this.modes.add(Mode.VALIDATE_CONSTRAINTS);
            }
            if (continueOnError) {
                this.modes.add(Mode.CONTINUE_ON_ERROR);
            }
        }

        public ShardInsertRequest newRequest(ShardId shardId) {
            return new ShardInsertRequest(
                shardId,
                jobId,
                modes,
                sessionSettings,
                insertColumns
            ).timeout(timeout);
        }
    }
}
