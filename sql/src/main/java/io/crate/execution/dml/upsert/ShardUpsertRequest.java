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
import io.crate.metadata.doc.DocSysColumns;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
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
    private Boolean isRawSourceInsert;
    private boolean validateConstraints = true;
    private boolean isRetry;

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

    ShardUpsertRequest() {
    }

    private ShardUpsertRequest(ShardId shardId,
                               @Nullable String[] updateColumns,
                               @Nullable Reference[] insertColumns,
                               UUID jobId) {
        super(shardId, jobId);
        assert updateColumns != null || insertColumns != null
            : "Missing updateAssignments, whether for update nor for insert";
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        if (insertColumns != null) {
            insertValuesStreamer = new Streamer[insertColumns.length];
            for (int i = 0; i < insertColumns.length; i++) {
                insertValuesStreamer[i] = insertColumns[i].valueType().streamer();
            }
        }
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

    Boolean isRawSourceInsert() {
        if (isRawSourceInsert == null) {
            assert insertColumns != null : "insertColumns must not be NULL on insert requests";
            isRawSourceInsert =
                insertColumns.length == 1 && insertColumns[0].column().equals(DocSysColumns.RAW);
        }
        return isRawSourceInsert;
    }

    /**
     * Returns <code>true</code> if this request has been sent to a shard copy more than once.
     */
    public boolean isRetry() {
        return isRetry;
    }

    @Override
    public void onRetry() {
        isRetry = true;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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

        int numItems = in.readVInt();
        readItems(in, numItems);
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

        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out, insertValuesStreamer);
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
        if (!super.equals(o)) return false;
        ShardUpsertRequest items = (ShardUpsertRequest) o;
        return continueOnError == items.continueOnError &&
               duplicateKeyAction == items.duplicateKeyAction &&
               validateConstraints == items.validateConstraints &&
               Objects.equal(isRawSourceInsert, items.isRawSourceInsert) &&
               Arrays.equals(updateColumns, items.updateColumns) &&
               Arrays.equals(insertColumns, items.insertColumns) &&
               Arrays.equals(insertValuesStreamer, items.insertValuesStreamer);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), continueOnError, duplicateKeyAction, isRawSourceInsert, validateConstraints, updateColumns, insertColumns, insertValuesStreamer);
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

        public Item(String id,
                    @Nullable Symbol[] updateAssignments,
                    @Nullable Object[] insertValues,
                    @Nullable Long version) {
            super(id);
            this.updateAssignments = updateAssignments;
            if (version != null) {
                this.version = version;
            }
            this.insertValues = insertValues;
        }

        @Nullable
        public BytesReference source() {
            return source;
        }

        public void source(BytesReference source) {
            this.source = source;
        }

        boolean retryOnConflict() {
            return version == Versions.MATCH_ANY;
        }

        @Nullable
        Symbol[] updateAssignments() {
            return updateAssignments;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
        }

        public Item(StreamInput in, @Nullable Streamer[] insertValueStreamers) throws IOException {
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
            }
        }

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
        }
    }

    public static class Builder {

        private final TimeValue timeout;
        private final DuplicateKeyAction duplicateKeyAction;
        private final boolean continueOnError;
        @Nullable
        private final String[] assignmentsColumns;
        @Nullable
        private final Reference[] missingAssignmentsColumns;
        private final UUID jobId;
        private boolean validateGeneratedColumns;

        public Builder(TimeValue timeout,
                       DuplicateKeyAction duplicateKeyAction,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       UUID jobId) {
            this(timeout, duplicateKeyAction, continueOnError, assignmentsColumns, missingAssignmentsColumns, jobId, true);
        }

        public Builder(DuplicateKeyAction duplicateKeyAction,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       UUID jobId,
                       boolean validateGeneratedColumns) {
            this(ReplicationRequest.DEFAULT_TIMEOUT, duplicateKeyAction, continueOnError,
                assignmentsColumns, missingAssignmentsColumns, jobId, validateGeneratedColumns);
        }

        public Builder(TimeValue timeout,
                       DuplicateKeyAction duplicateKeyAction,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       UUID jobId,
                       boolean validateGeneratedColumns) {
            this.timeout = timeout;
            this.duplicateKeyAction = duplicateKeyAction;
            this.continueOnError = continueOnError;
            this.assignmentsColumns = assignmentsColumns;
            this.missingAssignmentsColumns = missingAssignmentsColumns;
            this.jobId = jobId;
            this.validateGeneratedColumns = validateGeneratedColumns;
        }

        public ShardUpsertRequest newRequest(ShardId shardId) {
            return new ShardUpsertRequest(
                shardId,
                assignmentsColumns,
                missingAssignmentsColumns,
                jobId)
                .timeout(timeout)
                .continueOnError(continueOnError)
                .duplicateKeyAction(duplicateKeyAction)
                .validateConstraints(validateGeneratedColumns);
        }
    }
}
