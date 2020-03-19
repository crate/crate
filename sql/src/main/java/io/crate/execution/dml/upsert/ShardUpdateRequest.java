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
import io.crate.execution.dml.ShardRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
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

public class ShardUpdateRequest extends ShardRequest<ShardUpdateRequest, ShardUpdateRequest.Item> {

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
     * List of references or expressions to compute values for returning for update.
     */
    @Nullable
    private Symbol[] returnValues;

    ShardUpdateRequest() {
    }

    private ShardUpdateRequest(SessionSettings sessionSettings,
                               ShardId shardId,
                               @Nullable String[] updateColumns,
                               @Nullable Symbol[] returnValues,
                               UUID jobId) {
        super(shardId, jobId);
        assert updateColumns != null : "Missing updateColumns, whether for update nor for insert";
        this.sessionSettings = sessionSettings;
        this.updateColumns = updateColumns;
        this.returnValues = returnValues;
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

    DuplicateKeyAction duplicateKeyAction() {
        return duplicateKeyAction;
    }

    private ShardUpdateRequest duplicateKeyAction(DuplicateKeyAction duplicateKeyAction) {
        this.duplicateKeyAction = duplicateKeyAction;
        return this;
    }

    boolean continueOnError() {
        return continueOnError;
    }

    private ShardUpdateRequest continueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
        return this;
    }

    boolean validateConstraints() {
        return validateConstraints;
    }

    ShardUpdateRequest validateConstraints(boolean validateConstraints) {
        this.validateConstraints = validateConstraints;
        return this;
    }

    public ShardUpdateRequest(StreamInput in) throws IOException {
        super(in);
        int assignmentsColumnsSize = in.readVInt();
        if (assignmentsColumnsSize > 0) {
            updateColumns = new String[assignmentsColumnsSize];
            for (int i = 0; i < assignmentsColumnsSize; i++) {
                updateColumns[i] = in.readString();
            }
        }
        continueOnError = in.readBoolean();
        duplicateKeyAction = DuplicateKeyAction.values()[in.readVInt()];
        validateConstraints = in.readBoolean();

        sessionSettings = new SessionSettings(in);
        int numItems = in.readVInt();
        readItems(in, numItems);
        int returnValuesSize = in.readVInt();
        if (returnValuesSize > 0) {
            returnValues = new Symbol[returnValuesSize];
            for (int i = 0; i < returnValuesSize; i++) {
                returnValues[i] = Symbols.fromStream(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (updateColumns != null) {
            out.writeVInt(updateColumns.length);
            for (String column : updateColumns) {
                out.writeString(column);
            }
        } else {
            out.writeVInt(0);
        }

        out.writeBoolean(continueOnError);
        out.writeVInt(duplicateKeyAction.ordinal());
        out.writeBoolean(validateConstraints);

        sessionSettings.writeTo(out);

        out.writeVInt(items.size());
        for (ShardUpdateRequest.Item item : items) {
            item.writeTo(out);
        }
        if (returnValues != null) {
            out.writeVInt(returnValues.length);
            for (Symbol returnValue : returnValues) {
                Symbols.toStream(returnValue, out);
            }
        } else {
            out.writeVInt(0);
        }
    }

    @Override
    protected Item readItem(StreamInput input) throws IOException {
        return new Item(input);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;
        ShardUpdateRequest items = (ShardUpdateRequest) o;
        return continueOnError == items.continueOnError &&
               duplicateKeyAction == items.duplicateKeyAction &&
               validateConstraints == items.validateConstraints &&
               Arrays.equals(updateColumns, items.updateColumns) &&
               Arrays.equals(returnValues, items.returnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), continueOnError, duplicateKeyAction, validateConstraints, updateColumns, returnValues);
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
         * List of references or expressions to compute values for returning for update.
         */
        @Nullable
        private Symbol[] returnValues;

        public Item(String id,
                    @Nullable Symbol[] updateAssignments,
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
        public Symbol[] returnValues() {
            return returnValues;
        }

        public Item(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                int assignmentsSize = in.readVInt();
                updateAssignments = new Symbol[assignmentsSize];
                for (int i = 0; i < assignmentsSize; i++) {
                    updateAssignments[i] = Symbols.fromStream(in);
                }
            }
            if (in.readBoolean()) {
                source = in.readBytesReference();
            }
            if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
                int returnValueSize = in.readVInt();
                if (returnValueSize > 0) {
                    returnValues = new Symbol[returnValueSize];
                    for (int i = 0; i < returnValueSize; i++) {
                        returnValues[i] = Symbols.fromStream(in);
                    }
                }
            }
        }

        public void writeTo(StreamOutput out) throws IOException {
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

            boolean sourceAvailable = source != null;
            out.writeBoolean(sourceAvailable);
            if (sourceAvailable) {
                out.writeBytesReference(source);
            }
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

    public static class Builder {

        private final SessionSettings sessionSettings;
        private final TimeValue timeout;
        private final DuplicateKeyAction duplicateKeyAction;
        private final boolean continueOnError;
        @Nullable
        private final String[] assignmentsColumns;
        private final UUID jobId;
        private boolean validateGeneratedColumns;
        @Nullable
        private final Symbol[] returnValues;


        public Builder(SessionSettings sessionSettings,
                       TimeValue timeout,
                       DuplicateKeyAction duplicateKeyAction,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Symbol[] returnValue,
                       UUID jobId,
                       boolean validateGeneratedColumns) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.duplicateKeyAction = duplicateKeyAction;
            this.continueOnError = continueOnError;
            this.assignmentsColumns = assignmentsColumns;
            this.jobId = jobId;
            this.validateGeneratedColumns = validateGeneratedColumns;
            this.returnValues = returnValue;
        }

        public ShardUpdateRequest newRequest(ShardId shardId) {
            return new ShardUpdateRequest(
                sessionSettings,
                shardId,
                assignmentsColumns,
                returnValues,
                jobId)
                .timeout(timeout)
                .continueOnError(continueOnError)
                .duplicateKeyAction(duplicateKeyAction)
                .validateConstraints(validateGeneratedColumns);
        }
    }
}
