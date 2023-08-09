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

import static io.crate.analyze.AnalyzedAlterTableDropColumn.DropColumn;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;

import io.crate.metadata.RelationName;

public class DropColumnRequest extends AcknowledgedRequest<DropColumnRequest> {

    private final RelationName relationName;
    private final List<DropColumn> colsToDrop;

    public DropColumnRequest(@NotNull RelationName relationName,
                             @NotNull List<DropColumn> colsToDrop) {
        this.relationName = relationName;
        this.colsToDrop = colsToDrop;
    }

    public DropColumnRequest(StreamInput in) throws IOException {
        super(in);
        this.relationName = new RelationName(in);
        this.colsToDrop = in.readList(DropColumn::fromStream);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        out.writeCollection(colsToDrop, DropColumn::toStream);
    }

    @NotNull
    public RelationName relationName() {
        return this.relationName;
    }

    @NotNull
    public List<DropColumn> colsToDrop() {
        return this.colsToDrop;
    }
}
