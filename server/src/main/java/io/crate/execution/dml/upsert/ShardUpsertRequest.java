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

package io.crate.execution.dml.upsert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.common.unit.TimeValue;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.ShardRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;

public final class ShardUpsertRequest extends ShardRequest<ShardUpsertRequest, ShardUpsertRequest.Item> {

    public enum DuplicateKeyAction {
        UPDATE_OR_FAIL,
        OVERWRITE,
        IGNORE
    }

    private final DuplicateKeyAction duplicateKeyAction;
    private final boolean continueOnError;
    private boolean isRetry = false;
    private final SessionSettings sessionSettings;

    /**
     * List of column names used on update
     */
    @Nullable
    private String[] updateColumns;

    /**
     * List of references used on insert
     */
    @Nullable
    private Reference[] insertColumns;

    /**
     * List of references or expressions to compute values for returning for update.
     */
    @Nullable
    private Symbol[] returnValues;


    public ShardUpsertRequest(
        ShardId shardId,
        UUID jobId,
        boolean continueOnError,
        DuplicateKeyAction duplicateKeyAction,
        SessionSettings sessionSettings,
        @Nullable String[] updateColumns,
        @Nullable Reference[] insertColumns,
        @Nullable Symbol[] returnValues) {
        super(shardId, jobId);
        assert updateColumns != null || insertColumns != null : "Missing updateAssignments, whether for update nor for insert";
        this.continueOnError = continueOnError;
        this.duplicateKeyAction = duplicateKeyAction;
        this.sessionSettings = sessionSettings;
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        this.returnValues = returnValues;
    }

    public ShardUpsertRequest(StreamInput in) throws IOException {
        super(in);
        int assignmentsColumnsSize = in.readVInt();
        if (assignmentsColumnsSize > 0) {
            updateColumns = new String[assignmentsColumnsSize];
            for (int i = 0; i < assignmentsColumnsSize; i++) {
                updateColumns[i] = in.readString();
            }
        }
        int missingAssignmentsColumnsSize = in.readVInt();
        Streamer<?>[] insertValuesStreamer = null;
        if (missingAssignmentsColumnsSize > 0) {
            insertColumns = new Reference[missingAssignmentsColumnsSize];
            for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
                insertColumns[i] = Reference.fromStream(in);
            }
            insertValuesStreamer = Symbols.streamerArray(insertColumns);
        }
        continueOnError = in.readBoolean();
        duplicateKeyAction = DuplicateKeyAction.values()[in.readVInt()];
        if (in.getVersion().before(Version.V_5_5_0)) {
            in.readBoolean(); // validation
        }

        sessionSettings = new SessionSettings(in);
        int numItems = in.readVInt();
        items = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            items.add(new Item(in, insertValuesStreamer));
        }
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            int returnValuesSize = in.readVInt();
            if (returnValuesSize > 0) {
                returnValues = new Symbol[returnValuesSize];
                for (int i = 0; i < returnValuesSize; i++) {
                    returnValues[i] = Symbols.fromStream(in);
                }
            }
        }
        if (in.getVersion().onOrAfter(Version.V_4_8_0)) {
            isRetry = in.readBoolean();
        } else {
            isRetry = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // Stream References
        if (updateColumns != null) {
            out.writeVInt(updateColumns.length);
            for (String column : updateColumns) {
                out.writeString(column);
            }
        } else {
            out.writeVInt(0);
        }
        Streamer<?>[] insertValuesStreamer = null;
        if (insertColumns != null) {
            out.writeVInt(insertColumns.length);
            for (Reference reference : insertColumns) {
                Reference.toStream(out, reference);
            }
            insertValuesStreamer = Symbols.streamerArray(insertColumns);
        } else {
            out.writeVInt(0);
        }

        out.writeBoolean(continueOnError);
        out.writeVInt(duplicateKeyAction.ordinal());
        if (out.getVersion().before(Version.V_5_5_0)) {
            out.writeBoolean(true); // validation
        }

        sessionSettings.writeTo(out);

        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out, insertValuesStreamer);
        }
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            if (returnValues != null) {
                out.writeVInt(returnValues.length);
                for (Symbol returnValue : returnValues) {
                    Symbols.toStream(returnValue, out);
                }
            } else {
                out.writeVInt(0);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_4_8_0)) {
            out.writeBoolean(isRetry);
        }
    }

    @Override
    public void onRetry() {
        this.isRetry = true;
    }

    public boolean isRetry() {
        return isRetry;
    }

    @Nullable
    public SessionSettings sessionSettings() {
        return sessionSettings;
    }

    @Nullable
    public Symbol[] returnValues() {
        return returnValues;
    }

    @Nullable
    public String[] updateColumns() {
        return updateColumns;
    }

    @Nullable
    public Reference[] insertColumns() {
        return insertColumns;
    }

    public void insertColumns(Collection<Reference> columns) {
        this.insertColumns = columns.toArray(new Reference[0]);
    }

    public boolean continueOnError() {
        return continueOnError;
    }

    public DuplicateKeyAction duplicateKeyAction() {
        return duplicateKeyAction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ShardUpsertRequest items = (ShardUpsertRequest) o;
        return continueOnError == items.continueOnError &&
               Objects.equals(sessionSettings, items.sessionSettings) &&
               duplicateKeyAction == items.duplicateKeyAction &&
               Arrays.equals(updateColumns, items.updateColumns) &&
               Arrays.equals(insertColumns, items.insertColumns) &&
               Arrays.equals(returnValues, items.returnValues);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(),
                                  sessionSettings,
                                  duplicateKeyAction,
                                  continueOnError);
        result = 31 * result + Arrays.hashCode(updateColumns);
        result = 31 * result + Arrays.hashCode(insertColumns);
        result = 31 * result + Arrays.hashCode(returnValues);
        return result;
    }

    /**
     * A single update item.
     */
    public static final class Item extends ShardRequest.Item implements IndexItem {

        public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Item.class);

        @Nullable
        private BytesReference source;

        /**
         * List of symbols used on update if document exist
         */
        @Nullable
        private Symbol[] updateAssignments;

        /**
         * List of objects used on insert
         */
        @Nullable
        private Object[] insertValues;

        /**
         * Values that make up the primary key.
         */
        private List<String> pkValues;

        private final long autoGeneratedTimestamp;

        /**
         * Used to cache calculation for {@link #ramBytesUsed()}
         */
        private transient long usedBytes = -1L;

        public static Item forUpdate(String id,
                                     Symbol[] assignments,
                                     long requiredVersion,
                                     long seqNo,
                                     long primaryTerm) {
            return new Item(
                id,
                assignments,
                null,
                requiredVersion,
                seqNo,
                primaryTerm,
                List.of(),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP
            );
        }

        public static Item forInsert(String id,
                                     List<String> pkValues,
                                     long autoGeneratedTimestamp,
                                     @Nullable Object[] values,
                                     @Nullable Symbol[] onConflictAssignments) {
            return new Item(
                id,
                onConflictAssignments,
                values,
                Versions.MATCH_ANY,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                pkValues,
                autoGeneratedTimestamp
            );
        }

        Item(String id,
             @Nullable Symbol[] updateAssignments,
             @Nullable Object[] insertValues,
             long version,
             long seqNo,
             long primaryTerm,
             List<String> pkValues,
             long autoGeneratedTimestamp) {
            super(id);
            this.updateAssignments = updateAssignments;
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.insertValues = insertValues;
            this.pkValues = pkValues;
            this.autoGeneratedTimestamp = autoGeneratedTimestamp;
        }

        @Override
        public long ramBytesUsed() {
            if (usedBytes > -1) {
                return usedBytes;
            }
            usedBytes = super.ramBytesUsed()
                + Long.BYTES; // autoGeneratedTimestamp
            if (source != null) {
                usedBytes += source.length();
            }
            if (updateAssignments != null) {
                for (var assignment : updateAssignments) {
                    usedBytes += assignment.ramBytesUsed();
                }
            }
            if (insertValues != null) {
                for (var value : insertValues) {
                    usedBytes += RamUsageEstimator.sizeOfObject(value);
                }
            }
            for (String pkValue : pkValues) {
                usedBytes += RamUsageEstimator.sizeOf(pkValue);
            }
            return usedBytes;
        }

        @Nullable
        public BytesReference source() {
            return source;
        }

        public void source(BytesReference source) {
            this.source = source;
        }

        boolean retryOnConflict() {
            return seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && version == Versions.MATCH_ANY;
        }

        @Nullable
        Symbol[] updateAssignments() {
            return updateAssignments;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
        }

        public void insertValues(Object[] insertValues) {
            this.insertValues = insertValues;
        }

        public List<String> pkValues() {
            return pkValues;
        }

        public void pkValues(List<String> pkValues) {
            this.pkValues = pkValues;
        }

        public long autoGeneratedTimestamp() {
            return autoGeneratedTimestamp;
        }

        private static boolean streamPkValues(Version version) {
            return version.after(Version.V_4_7_2) && !version.equals(Version.V_4_8_0);
        }

        public Item(StreamInput in, @Nullable Streamer<?>[] insertValueStreamers) throws IOException {
            super(in);
            if (in.readBoolean()) {
                int assignmentsSize = in.readVInt();
                updateAssignments = new Symbol[assignmentsSize];
                for (int i = 0; i < assignmentsSize; i++) {
                    updateAssignments[i] = Symbols.fromStream(in);
                }
            }

            int missingAssignmentsSize = in.readVInt();
            if (missingAssignmentsSize > 0) {
                assert insertValueStreamers != null : "streamers are required if reading insert values";
                this.insertValues = new Object[missingAssignmentsSize];
                for (int i = 0; i < missingAssignmentsSize; i++) {
                    insertValues[i] = insertValueStreamers[i].readValueFrom(in);
                }
            }
            if (in.readBoolean()) {
                source = in.readBytesReference();
            } else if (in.getVersion().before(Version.V_5_3_0)) {
                // Below 5.3.0 a NULL source indicates a item to be skipped instead of the later introduced marker.
                seqNo = SequenceNumbers.SKIP_ON_REPLICA;
            }
            if (streamPkValues(in.getVersion())) {
                pkValues = in.readList(StreamInput::readString);
            } else {
                pkValues = List.of();
            }
            if (in.getVersion().onOrAfter(Version.V_5_0_0)) {
                autoGeneratedTimestamp = in.readLong();
            } else {
                autoGeneratedTimestamp = Translog.UNSET_AUTO_GENERATED_TIMESTAMP;
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public void writeTo(StreamOutput out, @Nullable Streamer[] insertValueStreamers) throws IOException {
            super.writeTo(out);
            if (updateAssignments != null) {
                out.writeBoolean(true);
                out.writeVInt(updateAssignments.length);
                for (Symbol updateAssignment : updateAssignments) {
                    Symbols.toStream(updateAssignment, out);
                }
            } else {
                out.writeBoolean(false);
            }
            // Stream References
            if (insertValues != null) {
                assert insertValueStreamers != null && insertValueStreamers.length >= insertValues.length
                    : "streamers are required to stream insert values and must have a streamer for each value";
                out.writeVInt(insertValues.length);
                for (int i = 0; i < insertValues.length; i++) {
                    insertValueStreamers[i].writeValueTo(out, insertValues[i]);
                }
            } else {
                out.writeVInt(0);
            }
            boolean sourceAvailable = source != null;
            out.writeBoolean(sourceAvailable);
            if (sourceAvailable) {
                out.writeBytesReference(source);
            }
            if (streamPkValues(out.getVersion())) {
                out.writeStringCollection(pkValues);
            }
            if (out.getVersion().onOrAfter(Version.V_5_0_0)) {
                out.writeLong(autoGeneratedTimestamp);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            Item item = (Item) o;
            return Objects.equals(source, item.source) &&
                   Arrays.equals(updateAssignments, item.updateAssignments) &&
                   Arrays.equals(insertValues, item.insertValues);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(super.hashCode(), source);
            result = 31 * result + Arrays.hashCode(updateAssignments);
            result = 31 * result + Arrays.hashCode(insertValues);
            return result;
        }

    }

    public static class Builder {

        private final SessionSettings sessionSettings;
        private final TimeValue timeout;
        private final DuplicateKeyAction duplicateKeyAction;
        private final boolean continueOnError;
        @Nullable
        private final String[] assignmentsColumns;
        @Nullable
        private final Reference[] missingAssignmentsColumns;
        private final UUID jobId;
        @Nullable
        private final Symbol[] returnValues;

        public Builder(SessionSettings sessionSettings,
                       TimeValue timeout,
                       DuplicateKeyAction duplicateKeyAction,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       @Nullable Symbol[] returnValue,
                       UUID jobId) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.duplicateKeyAction = duplicateKeyAction;
            this.continueOnError = continueOnError;
            this.assignmentsColumns = assignmentsColumns;
            this.missingAssignmentsColumns = missingAssignmentsColumns;
            this.jobId = jobId;
            this.returnValues = returnValue;
        }

        public ShardUpsertRequest newRequest(ShardId shardId) {
            return new ShardUpsertRequest(
                shardId,
                jobId,
                continueOnError,
                duplicateKeyAction,
                sessionSettings,
                assignmentsColumns,
                missingAssignmentsColumns,
                returnValues
            ).timeout(timeout);
        }
    }

}
