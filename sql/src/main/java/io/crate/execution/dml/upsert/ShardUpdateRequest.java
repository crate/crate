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

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public final class ShardUpdateRequest extends ShardWriteRequest<ShardUpdateRequest, ShardUpdateRequest.Item> {

    /**
     * List of column names used on update
     */
    private String[] updateColumns;

    private SessionSettings sessionSettings;

    @Nullable
    private Symbol[] returnValues;

    public ShardUpdateRequest(
        ShardId shardId,
        UUID jobId,
        EnumSet<Mode> modes,
        SessionSettings sessionSettings,
        String[] updateColumns,
        @Nullable Symbol[] returnValues
    ) {
        super(shardId, jobId, modes);
        this.sessionSettings = sessionSettings;
        this.updateColumns = updateColumns;
        this.returnValues = returnValues;
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
        sessionSettings = new SessionSettings(in);
        int numItems = in.readVInt();
        items = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            items.add(new ShardUpdateRequest.Item(in));
        }
        int returnValuesSize = in.readVInt();
        if (returnValuesSize > 0) {
            returnValues = new Symbol[returnValuesSize];
            for (int i = 0; i < returnValuesSize; i++) {
                returnValues[i] = Symbols.fromStream(in);
            }
        }
    }

    @Override
    public SessionSettings sessionSettings() {
        return sessionSettings;
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
    @Nullable
    public Symbol[] returnValues() {
        return returnValues;
    }

    @Override
    public String[] updateColumns() {
        return updateColumns;
    }

    @Override
    @Nullable
    public Reference[] insertColumns() {
        return null;
    }

    public static class Item extends ShardWriteRequest.Item {

        /**
         * List of symbols used on update if document exist
         */
        @Nullable
        private Symbol[] updateAssignments;


        protected Item(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                int assignmentsSize = in.readVInt();
                updateAssignments = new Symbol[assignmentsSize];
                for (int i = 0; i < assignmentsSize; i++) {
                    updateAssignments[i] = Symbols.fromStream(in);
                }
            }
        }

        public Item(String id,
                    Symbol[] updateAssignments,
                    @Nullable Long version,
                    @Nullable Long seqNo,
                    @Nullable Long primaryTerm
        ) {
            super(id, version, seqNo, primaryTerm);
            this.updateAssignments = updateAssignments;
        }

        @Nullable
        @Override
        Symbol[] updateAssignments() {
            return updateAssignments;
        }

        @Nullable
        @Override
        public Object[] insertValues() {
            return null;
        }

        @Override
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
        }
    }

    public static class Builder {

        private final SessionSettings sessionSettings;
        private final TimeValue timeout;
        private final String[] assignmentsColumns;
        @Nullable
        private final Symbol[] returnValues;
        private final UUID jobId;
        private final EnumSet<Mode> modes;

        public Builder(SessionSettings sessionSettings,
                       TimeValue timeout,
                       boolean continueOnError,
                       String[] assignmentsColumns,
                       Symbol[] returnValues,
                       UUID jobId,
                       boolean validateGeneratedColumns,
                       Mode... modes) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.assignmentsColumns = assignmentsColumns;
            this.returnValues = returnValues;
            this.jobId = jobId;
            this.modes = EnumSet.copyOf(List.of(modes));
            if (validateGeneratedColumns) {
                this.modes.add(Mode.VALIDATE_CONSTRAINTS);
            }
            if (continueOnError) {
                this.modes.add(Mode.CONTINUE_ON_ERROR);
            }
        }

        public ShardUpdateRequest newRequest(ShardId shardId) {
            return new ShardUpdateRequest(
                shardId,
                jobId,
                modes,
                sessionSettings,
                assignmentsColumns,
                returnValues
            ).timeout(timeout);
        }
    }
}
