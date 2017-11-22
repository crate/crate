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

package io.crate.planner.node.dml;

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.UnnestablePlan;

import java.util.Map;
import java.util.UUID;

public final class UpdateById extends UnnestablePlan {

    private final UUID jobId;
    private final DocTableInfo table;
    private final Map<Reference, Symbol> assignmentByTargetCol;
    private final DocKeys docKeys;

    public UpdateById(UUID jobId, DocTableInfo table, Map<Reference, Symbol> assignmentByTargetCol, DocKeys docKeys) {
        this.jobId = jobId;
        this.table = table;
        this.assignmentByTargetCol = assignmentByTargetCol;
        this.docKeys = docKeys;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitUpdateById(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }

    public DocKeys docKeys() {
        return docKeys;
    }

    public DocTableInfo table() {
        return table;
    }

    public Map<Reference, Symbol> assignmentByTargetCol() {
        return assignmentByTargetCol;
    }
}
