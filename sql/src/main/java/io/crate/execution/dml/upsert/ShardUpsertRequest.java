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

import com.google.common.base.Objects;
import io.crate.Streamer;
import io.crate.execution.dml.ShardRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class ShardUpsertRequest extends ShardRequest<ShardUpsertRequest, ShardUpsertRequest.Item> {

    public enum DuplicateKeyAction {
        UPDATE_OR_FAIL,
        OVERWRITE,
        IGNORE
    }

    private DuplicateKeyAction duplicateKeyAction;
    private boolean continueOnError;
    private boolean validateConstraints = true;
    private SessionSettings sessionSettings;

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
     * List of data type streamer resolved through insertColumns
     */
    @Nullable
    private Streamer[] insertValuesStreamer;

    /**
     * List of references or expressions to compute values for returning for update.
     */
    @Nullable
    private Symbol[] returnValues;

    private boolean allOn4_2;

    ShardUpsertRequest() {
    }

    private ShardUpsertRequest(SessionSettings sessionSettings,
                               ShardId shardId,
                               Version version,
                               @Nullable String[] updateColumns,
                               @Nullable Reference[] insertColumns,
                               @Nullable Symbol[] returnValues,
                               UUID jobId) {
        super(shardId, jobId);
        assert updateColumns != null || insertColumns != null
            : "Missing updateAssignments, whether for update nor for insert";
        this.sessionSettings = sessionSettings;
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        if (insertColumns != null) {
            insertValuesStreamer = new Streamer[insertColumns.length];
            for (int i = 0; i < insertColumns.length; i++) {
                insertValuesStreamer[i] = insertColumns[i].valueType().streamer();
            }
        }
        this.returnValues = returnValues;
        this.allOn4_2 = version.onOrAfter(Version.V_4_2_0);
    }

    public SessionSettings sessionSettings() {
        return sessionSettings;
    }


    @Nullable
    public Symbol[] getReturnValues() {
        return returnValues;
    }

    String[] updateColumns() {
        return updateColumns;
    }

    @Nullable
    Reference[] insertColumns() {
        return insertColumns;
    }

    DuplicateKeyAction duplicateKeyAction() {
        return duplicateKeyAction;
    }

    private ShardUpsertRequest duplicateKeyAction(DuplicateKeyAction duplicateKeyAction) {
        this.duplicateKeyAction = duplicateKeyAction;
        return this;
    }

    boolean continueOnError() {
        return continueOnError;
    }

    private ShardUpsertRequest continueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
        return this;
    }

    boolean validateConstraints() {
        return validateConstraints;
    }

    ShardUpsertRequest validateConstraints(boolean validateConstraints) {
        this.validateConstraints = validateConstraints;
        return this;
    }

    public ShardUpsertRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            this.allOn4_2 = in.readBoolean();
        } else {
            this.allOn4_2 = false;
        }
        int assignmentsColumnsSize = in.readVInt();
        if (assignmentsColumnsSize > 0) {
            updateColumns = new String[assignmentsColumnsSize];
            for (int i = 0; i < assignmentsColumnsSize; i++) {
                updateColumns[i] = in.readString();
            }
        }
        int missingAssignmentsColumnsSize = in.readVInt();
        if (missingAssignmentsColumnsSize > 0) {
            insertColumns = new Reference[missingAssignmentsColumnsSize];
            insertValuesStreamer = new Streamer[missingAssignmentsColumnsSize];
            for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
                insertColumns[i] = Reference.fromStream(in);
                insertValuesStreamer[i] = insertColumns[i].valueType().streamer();
            }
        }
        continueOnError = in.readBoolean();
        duplicateKeyAction = DuplicateKeyAction.values()[in.readVInt()];
        validateConstraints = in.readBoolean();

        sessionSettings = new SessionSettings(in);

        int numItems = in.readVInt();
        readItems(in, numItems);
        if (allOn4_2) {
            int returnValuesSize = in.readVInt();
            if (returnValuesSize > 0) {
                returnValues = new Symbol[returnValuesSize];
                for (int i = 0; i < returnValuesSize; i++) {
                    returnValues[i] = Symbols.fromStream(in);
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            out.writeBoolean(allOn4_2);
        }
        // Stream References
        if (updateColumns != null) {
            out.writeVInt(updateColumns.length);
            for (String column : updateColumns) {
                out.writeString(column);
            }
        } else {
            out.writeVInt(0);
        }
        if (insertColumns != null) {
            out.writeVInt(insertColumns.length);
            for (Reference reference : insertColumns) {
                Reference.toStream(reference, out);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeBoolean(continueOnError);
        out.writeVInt(duplicateKeyAction.ordinal());
        out.writeBoolean(validateConstraints);

        sessionSettings.writeTo(out);

        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out, insertValuesStreamer, allOn4_2);
        }
        if (allOn4_2) {
            if (returnValues != null) {
                out.writeVInt(returnValues.length);
                for (Symbol returnValue : returnValues) {
                    Symbols.toStream(returnValue, out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    @Override
    protected Item readItem(StreamInput input) throws IOException {
        return new Item(input, insertValuesStreamer);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;
        ShardUpsertRequest items = (ShardUpsertRequest) o;
        return continueOnError == items.continueOnError &&
               duplicateKeyAction == items.duplicateKeyAction &&
               validateConstraints == items.validateConstraints &&
               Arrays.equals(updateColumns, items.updateColumns) &&
               Arrays.equals(insertColumns, items.insertColumns) &&
               Arrays.equals(insertValuesStreamer, items.insertValuesStreamer) &&
               Arrays.equals(returnValues, items.returnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), continueOnError, duplicateKeyAction, validateConstraints, updateColumns, insertColumns, insertValuesStreamer, returnValues);
    }

    /**
     * A single update item.
     */
    public static class Item extends ShardRequest.Item {

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
         * List of references or expressions to compute values for returning for update.
         */
        @Nullable
        private Symbol[] returnValues;

        public Item(String id,
                    @Nullable Symbol[] updateAssignments,
                    @Nullable Object[] insertValues,
                    @Nullable Long version,
                    @Nullable Long seqNo,
                    @Nullable Long primaryTerm,
                    @Nullable Symbol[] returnValues
                    ) {
            super(id);
            this.updateAssignments = updateAssignments;
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
            this.returnValues = returnValues;
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

        @Nullable
        public Symbol[] returnValues() {
            return returnValues;
        }

        public Item(StreamInput in, @Nullable Streamer[] insertValueStreamers) throws IOException {
            super(in);
            boolean allOn4_2 = false;
            if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
                allOn4_2 = in.readBoolean();
            }
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
            }
            if (allOn4_2) {
                int returnValueSize = in.readVInt();
                if (returnValueSize > 0) {
                    returnValues = new Symbol[returnValueSize];
                    for (int i = 0; i < returnValueSize; i++) {
                        returnValues[i] = Symbols.fromStream(in);
                    }
                }
            }
        }

        public void writeTo(StreamOutput out, @Nullable Streamer[] insertValueStreamers, boolean allOn4_2) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
                out.writeBoolean(allOn4_2);
            }
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
                assert insertValueStreamers != null : "streamers are required to stream insert values";
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

            if (allOn4_2) {
                if (returnValues != null) {
                    out.writeVInt(returnValues.length);
                    for (Symbol returnValue : returnValues) {
                        Symbols.toStream(returnValue, out);
                    }
                } else {
                    out.writeVInt(0);
                }
            }
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
        private boolean validateGeneratedColumns;
        @Nullable
        private final Symbol[] returnValues;

        private final Version version;

        public Builder(SessionSettings sessionSettings,
                       TimeValue timeout,
                       DuplicateKeyAction duplicateKeyAction,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       @Nullable Symbol[] returnValue,
                       UUID jobId,
                       boolean validateGeneratedColumns,
                       Version version) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.duplicateKeyAction = duplicateKeyAction;
            this.continueOnError = continueOnError;
            this.assignmentsColumns = assignmentsColumns;
            this.missingAssignmentsColumns = missingAssignmentsColumns;
            this.jobId = jobId;
            this.validateGeneratedColumns = validateGeneratedColumns;
            this.returnValues = returnValue;
            this.version = version;
        }

        public ShardUpsertRequest newRequest(ShardId shardId) {
            return new ShardUpsertRequest(
                sessionSettings,
                shardId,
                version,
                assignmentsColumns,
                missingAssignmentsColumns,
                returnValues,
                jobId)
                .timeout(timeout)
                .continueOnError(continueOnError)
                .duplicateKeyAction(duplicateKeyAction)
                .validateConstraints(validateGeneratedColumns);
        }
    }
}
