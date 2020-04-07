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

import io.crate.execution.dml.ShardRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


public abstract class ShardWriteRequest<T extends ShardRequest<T, I>, I extends ShardRequest.Item> extends ShardRequest<T, I> {

    public enum DuplicateKeyAction {
        UPDATE_OR_FAIL,
        OVERWRITE,
        IGNORE
    }

    protected enum Mode {
        DUPLICATE_KEY_UPDATE_OR_FAIL,
        DUPLICATE_KEY_OVERWRITE,
        DUPLICATE_KEY_IGNORE,
        CONTINUE_ON_ERROR,
        VALIDATE_CONSTRAINTS;

        static EnumSet<Mode> toEnumSet(boolean continueOnError, boolean validateConstraints, DuplicateKeyAction action) {
            Set<ShardWriteRequest.Mode> modes = new HashSet<>();
            if (continueOnError) {
                modes.add(Mode.CONTINUE_ON_ERROR);
            }
            if (validateConstraints) {
                modes.add(Mode.VALIDATE_CONSTRAINTS);
            }
            switch (action) {
                case IGNORE:
                    modes.add(Mode.DUPLICATE_KEY_IGNORE);
                    break;
                case OVERWRITE:
                    modes.add(Mode.DUPLICATE_KEY_OVERWRITE);
                    break;
                case UPDATE_OR_FAIL:
                    modes.add(Mode.DUPLICATE_KEY_UPDATE_OR_FAIL);
                    break;
                default:
                    throw new IllegalArgumentException("Action not supported for serialization " + action.name());
            }
            return EnumSet.copyOf(modes);
        }
    }

    protected ShardWriteRequest(StreamInput in) throws IOException {
        super(in);
    }

    protected ShardWriteRequest(ShardId shardId, UUID jobId) {
        super(shardId, jobId);
    }

    @Nullable
    public abstract SessionSettings sessionSettings();

    @Nullable
    public abstract Symbol[] returnValues();

    @Nullable
    public abstract String[] updateColumns();

    @Nullable
    public abstract Reference[] insertColumns();

    public abstract boolean continueOnError();

    public abstract boolean validateConstraints();

    public abstract DuplicateKeyAction duplicateKeyAction();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    /**
     * A single update item.
     */
    public abstract static class Item extends ShardRequest.Item {

        protected Item(StreamInput in) throws IOException {
            super(in);
        }

        protected Item(String id,
                    @Nullable Long version,
                    @Nullable Long seqNo,
                    @Nullable Long primaryTerm
        ) {
            super(id);
            if (version != null) {
                this.version = version;
            }
            if (seqNo != null) {
                this.seqNo = seqNo;
            }
            if (primaryTerm != null) {
                this.primaryTerm = primaryTerm;
            }
        }

        @Nullable
        public abstract BytesReference source();

        public abstract void source(BytesReference source);

        boolean retryOnConflict() {
            return seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && version == Versions.MATCH_ANY;
        }

        @Nullable
        abstract Symbol[] updateAssignments();

        @Nullable
        public abstract Object[] insertValues();


        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
