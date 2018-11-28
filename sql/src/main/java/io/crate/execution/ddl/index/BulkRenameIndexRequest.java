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

package io.crate.execution.ddl.index;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class BulkRenameIndexRequest extends AcknowledgedRequest<BulkRenameIndexRequest> {

    public static class RenameIndexAction {

        private final String sourceIndexName;
        private final String targetIndexName;

        public RenameIndexAction(@Nonnull String sourceIndexName,
                                 @Nonnull String targetIndexName) {
            this.sourceIndexName = Objects.requireNonNull(sourceIndexName,
                "Source Index Name must not be null");
            this.targetIndexName = Objects.requireNonNull(targetIndexName,
                "Target Index Name must not be null");
            if (sourceIndexName.isEmpty() || targetIndexName.isEmpty()) {
                throw new IllegalStateException("Source or Target Index Names must not be empty");
            }
        }

        public @Nonnull String sourceIndexName() {
            return sourceIndexName;
        }

        public @Nonnull String targetIndexName() {
            return targetIndexName;
        }
    }

    private List<RenameIndexAction> renameIndexActions;

    BulkRenameIndexRequest() {
        this.renameIndexActions = new ArrayList<>();
    }

    public BulkRenameIndexRequest(List<RenameIndexAction> renameIndexActions) {
        this.renameIndexActions = renameIndexActions;
    }

    private void addRenameIndexAction(@Nonnull String sourceIndex,
                                      @Nonnull String targetIndex) {
        renameIndexActions.add(new RenameIndexAction(sourceIndex, targetIndex));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (renameIndexActions.isEmpty()) {
            validationException = addValidationError("Rename Index Action list must not be empty", null);
        }
        return validationException;
    }

    public List<RenameIndexAction> renameIndexActions() {
        return renameIndexActions;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            addRenameIndexAction(in.readString(), in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(renameIndexActions.size());
        for (RenameIndexAction action : renameIndexActions) {
            out.writeString(action.sourceIndexName());
            out.writeString(action.targetIndexName());
        }
    }
}
