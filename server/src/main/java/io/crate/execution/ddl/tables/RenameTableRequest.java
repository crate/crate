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

package io.crate.execution.ddl.tables;

import java.io.IOException;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.RelationName;

public class RenameTableRequest extends AcknowledgedRequest<RenameTableRequest> {

    private final RelationName sourceName;
    private final RelationName targetName;
    private final boolean isPartitioned;

    public RenameTableRequest(RelationName sourceName, RelationName targetName, boolean isPartitioned) {
        this.sourceName = sourceName;
        this.targetName = targetName;
        this.isPartitioned = isPartitioned;
    }

    public RelationName sourceName() {
        return sourceName;
    }

    public RelationName targetName() {
        return targetName;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public RenameTableRequest(StreamInput in) throws IOException {
        super(in);
        sourceName = new RelationName(in);
        targetName = new RelationName(in);
        isPartitioned = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceName.writeTo(out);
        targetName.writeTo(out);
        out.writeBoolean(isPartitioned);
    }
}
