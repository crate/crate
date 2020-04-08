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
import io.crate.common.collections.EnumSets;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class ShardInsertRequest extends ShardWriteRequest<ShardInsertRequest, ShardInsertRequest.Item> {

    private SessionSettings sessionSettings;

    /**
     * List of references used on insert
     */
    private Reference[] insertColumns;

    private EnumSet<Values> values;

    public ShardInsertRequest(
        ShardId shardId,
        UUID jobId,
        SessionSettings sessionSettings,
        Reference[] insertColumns,
        boolean continueOnError,
        boolean validateGeneratedColumns,
        DuplicateKeyAction duplicateKeyAction) {
        super(shardId, jobId);
        this.sessionSettings = sessionSettings;
        this.insertColumns = insertColumns;
        this.values = Values.toEnumSet(continueOnError, validateGeneratedColumns, duplicateKeyAction);
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
        values = EnumSets.unpackFromInt(in.readVInt(), Values.class);
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
        out.writeVInt(EnumSets.packToInt(values));
        sessionSettings.writeTo(out);
        out.writeVInt(items.size());
        for (ShardInsertRequest.Item item : items) {
            item.writeTo(out, insertValuesStreamer);
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

    @Override
    public boolean continueOnError() {
        return Values.continueOnError(values);
    }

    @Override
    public boolean validateConstraints() {
        return Values.validateConstraints(values);
    }

    @Override
    public DuplicateKeyAction duplicateKeyAction() {
        return Values.getDuplicateAction(values);
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
        ShardInsertRequest items = (ShardInsertRequest) o;
        return Objects.equals(sessionSettings, items.sessionSettings) &&
               Arrays.equals(insertColumns, items.insertColumns) &&
               Objects.equals(values, items.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), sessionSettings, values);
        result = 31 * result + Arrays.hashCode(insertColumns);
        return result;
    }

    // Values is only used for internal storage and serialization
    private enum Values {
        DUPLICATE_KEY_UPDATE_OR_FAIL,
        DUPLICATE_KEY_OVERWRITE,
        DUPLICATE_KEY_IGNORE,
        CONTINUE_ON_ERROR,
        VALIDATE_CONSTRAINTS;

        static EnumSet<Values> toEnumSet(boolean continueOnError, boolean validateConstraints, DuplicateKeyAction action) {
            HashSet<Values> values = new HashSet<>();
            if (continueOnError) {
                values.add(Values.CONTINUE_ON_ERROR);
            }
            if (validateConstraints) {
                values.add(Values.VALIDATE_CONSTRAINTS);
            }
            switch (action) {
                case IGNORE:
                    values.add(Values.DUPLICATE_KEY_IGNORE);
                    break;
                case OVERWRITE:
                    values.add(Values.DUPLICATE_KEY_OVERWRITE);
                    break;
                case UPDATE_OR_FAIL:
                    values.add(Values.DUPLICATE_KEY_UPDATE_OR_FAIL);
                    break;
                default:
                    throw new IllegalArgumentException("DuplicateKeyAction not supported for serialization: " + action.name());
            }
            return EnumSet.copyOf(values);
        }

        static DuplicateKeyAction getDuplicateAction(EnumSet<Values> values) {
            if (values.contains(Values.DUPLICATE_KEY_UPDATE_OR_FAIL)) {
                return DuplicateKeyAction.UPDATE_OR_FAIL;
            }
            if (values.contains(Values.DUPLICATE_KEY_OVERWRITE)) {
                return DuplicateKeyAction.OVERWRITE;
            }
            if (values.contains(Values.DUPLICATE_KEY_IGNORE)) {
                return DuplicateKeyAction.IGNORE;
            }
            throw new IllegalArgumentException("DuplicateKeyAction found");
        }

        static boolean continueOnError(EnumSet<Values> values) {
            return values.contains(Values.CONTINUE_ON_ERROR);
        }

        static boolean validateConstraints(EnumSet<Values> values) {
            return values.contains(Values.VALIDATE_CONSTRAINTS);
        }
    }

    /**
     * A single insert item.
     */
    public static final class Item extends ShardWriteRequest.Item {

        @Nullable
        protected BytesReference source;

        /**
         * List of objects used on insert
         */
        @Nullable
        private Object[] insertValues;

        public Item(String id,
                    Object[] insertValues,
                    @Nullable Long version,
                    @Nullable Long seqNo,
                    @Nullable Long primaryTerm
        ) {
            super(id, version, seqNo, primaryTerm);
            this.insertValues = insertValues;
        }

        @Nullable
        @Override
        public BytesReference source() {
            return source;
        }

        @Override
        public void source(BytesReference source) {
            this.source = source;
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

        public Item(StreamInput in, Streamer[] insertValueStreamers) throws IOException {
            super(in);
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

        public void writeTo(StreamOutput out, Streamer[] insertValueStreamers) throws IOException {
            super.writeTo(out);
            assert insertValueStreamers != null : "streamers are required to stream insert values";
            out.writeVInt(insertValues.length);
            for (int i = 0; i < insertValues.length; i++) {
                insertValueStreamers[i].writeValueTo(out, insertValues[i]);
            }
            boolean sourceAvailable = source != null;
            out.writeBoolean(sourceAvailable);
            if (sourceAvailable) {
                out.writeBytesReference(source);
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
                   Arrays.equals(insertValues, item.insertValues);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(super.hashCode(), source);
            result = 31 * result + Arrays.hashCode(insertValues);
            return result;
        }
    }

    public static class Builder {

        private final SessionSettings sessionSettings;
        private final TimeValue timeout;
        private final Reference[] insertColumns;
        private final UUID jobId;
        private final boolean validateGeneratedColumns;
        private final boolean continueOnError;
        private final DuplicateKeyAction duplicateKeyAction;

        public Builder(
            SessionSettings sessionSettings,
            TimeValue timeout,
            DuplicateKeyAction duplicateKeyAction,
            boolean continueOnError,
            Reference[] insertColumns,
            UUID jobId,
            boolean validateGeneratedColumns) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.insertColumns = insertColumns;
            this.jobId = jobId;
            this.validateGeneratedColumns = validateGeneratedColumns;
            this.continueOnError = continueOnError;
            this.duplicateKeyAction = duplicateKeyAction;

        }

        public ShardInsertRequest newRequest(ShardId shardId) {
            return new ShardInsertRequest(
                shardId,
                jobId,
                sessionSettings,
                insertColumns,
                continueOnError,
                validateGeneratedColumns,
                duplicateKeyAction
            ).timeout(timeout);
        }
    }
}
