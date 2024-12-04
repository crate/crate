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
import org.jetbrains.annotations.NotNull;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class RenameColumnRequest extends AcknowledgedRequest<RenameColumnRequest> {

    private final RelationName relationName;
    private final Reference refToRename;
    private final ColumnIdent newName;

    public RenameColumnRequest(@NotNull RelationName relationName, @NotNull Reference refToRename, @NotNull ColumnIdent newName) {
        this.relationName = relationName;
        this.refToRename = refToRename;
        this.newName = newName;
    }

    public RenameColumnRequest(StreamInput in) throws IOException {
        super(in);
        this.relationName = new RelationName(in);
        this.refToRename = (Reference) Symbol.fromStream(in);
        this.newName = ColumnIdent.of(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        Symbol.toStream(refToRename, out);
        newName.writeTo(out);
    }

    public RelationName relationName() {
        return relationName;
    }

    public Reference refToRename() {
        return refToRename;
    }

    public ColumnIdent newName() {
        return newName;
    }
}
