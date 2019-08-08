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

package io.crate.execution.ddl.tables;

import io.crate.metadata.RelationName;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class RenameTableRequest extends AcknowledgedRequest<RenameTableRequest> {

    private final RelationName sourceRelationName;
    private final RelationName targetRelationName;
    private final boolean isPartitioned;

    public RenameTableRequest(RelationName sourceRelationName, RelationName targetRelationName, boolean isPartitioned) {
        this.sourceRelationName = sourceRelationName;
        this.targetRelationName = targetRelationName;
        this.isPartitioned = isPartitioned;
    }

    public RelationName sourceTableIdent() {
        return sourceRelationName;
    }

    public RelationName targetTableIdent() {
        return targetRelationName;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceRelationName == null || targetRelationName == null) {
            validationException = addValidationError("source and target table ident must not be null", null);
        }
        return validationException;
    }

    public RenameTableRequest(StreamInput in) throws IOException {
        super(in);
        sourceRelationName = new RelationName(in);
        targetRelationName = new RelationName(in);
        isPartitioned = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceRelationName.writeTo(out);
        targetRelationName.writeTo(out);
        out.writeBoolean(isPartitioned);
    }
}
