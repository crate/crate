/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.node.dml;

import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.symbol.Field;

import javax.annotation.Nullable;
import java.util.List;

public class UpdateNode extends DMLPlanNode implements PlannedAnalyzedRelation {

    private final List<List<DQLPlanNode>> nodes;

    public UpdateNode(List<List<DQLPlanNode>> nodes) {
        this.nodes = nodes;
    }

    public List<List<DQLPlanNode>> nodes() {
        return nodes;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitPlanedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(ColumnIdent path) {
        return null;
    }

    @Override
    public Field getWritableField(ColumnIdent path) throws UnsupportedOperationException, ColumnUnknownException {
        return null;
    }

    @Override
    public List<Field> fields() {
        return null;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitUpdateNode(this, context);
    }
}
