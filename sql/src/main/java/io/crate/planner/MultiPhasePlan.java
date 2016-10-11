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

package io.crate.planner;

import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.projection.Projection;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

/**
 * Plan which depends on other plans to be executed first.
 *
 * E.g. for sub selects:
 *
 * <pre>
 * select * from t where x = (select 1) or x = (select 2)
 * </pre>
 *
 * The two subselects would be the dependencies.
 *
 * The dependencies themselves may also be MultiPhasePlans.
 */
public class MultiPhasePlan implements PlannedAnalyzedRelation, Plan {


    private final PlannedAnalyzedRelation plannedAnalyzedRelation;
    private final List<Plan> dependencies;
    private final List<Symbol> subSelectParents;

    public MultiPhasePlan(PlannedAnalyzedRelation plannedAnalyzedRelation,
                          List<Plan> dependencies,
                          List<Symbol> subSelectParents) {
        this.plannedAnalyzedRelation = plannedAnalyzedRelation;
        this.dependencies = dependencies;
        this.subSelectParents = subSelectParents;
    }

    public List<Plan> dependencies() {
        return dependencies;
    }

    public Plan rootPlan() {
        return plannedAnalyzedRelation.plan();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitMultiPhasePlan(this, context);
    }

    @Override
    public UUID jobId() {
        return plannedAnalyzedRelation.plan().jobId();
    }

    @Override
    public Plan plan() {
        return this;
    }

    @Override
    public void addProjection(Projection projection) {
        plannedAnalyzedRelation.addProjection(projection);
    }

    @Override
    public boolean resultIsDistributed() {
        return plannedAnalyzedRelation.resultIsDistributed();
    }

    @Override
    public UpstreamPhase resultPhase() {
        return plannedAnalyzedRelation.resultPhase();
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return plannedAnalyzedRelation.accept(visitor, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        return plannedAnalyzedRelation.getField(path, operation);
    }

    @Override
    public List<Field> fields() {
        return plannedAnalyzedRelation.fields();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return plannedAnalyzedRelation.getQualifiedName();
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        plannedAnalyzedRelation.setQualifiedName(qualifiedName);
    }

    public List<Symbol> subSelectParents() {
        return subSelectParents;
    }
}
