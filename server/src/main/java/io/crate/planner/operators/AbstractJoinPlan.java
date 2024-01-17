/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.operators;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.common.collections.Lists;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.sql.tree.JoinType;

public abstract class AbstractJoinPlan implements LogicalPlan {

    protected final LogicalPlan lhs;
    protected final LogicalPlan rhs;
    @Nullable
    protected final Symbol joinCondition;
    protected final JoinType joinType;

    protected AbstractJoinPlan(
                               LogicalPlan lhs,
                               LogicalPlan rhs,
                               @Nullable Symbol joinCondition,
                               JoinType joinType) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinCondition = joinCondition;
        this.joinType = joinType;
    }

    public LogicalPlan lhs() {
        return lhs;
    }

    public LogicalPlan rhs() {
        return rhs;
    }

    @Override
    public List<Symbol> outputs() {
        if (joinType == JoinType.SEMI) {
            return lhs.outputs();
        } else {
            return Lists.concat(lhs.outputs(), rhs.outputs());
        }
    }


    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        Map<LogicalPlan, SelectSymbol> leftDeps = lhs.dependencies();
        Map<LogicalPlan, SelectSymbol> rightDeps = rhs.dependencies();
        HashMap<LogicalPlan, SelectSymbol> deps = new HashMap<>(leftDeps.size() + rightDeps.size());
        deps.putAll(leftDeps);
        deps.putAll(rightDeps);
        return deps;
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Override
    public List<RelationName> getRelationNames() {
        return Lists.concatUnique(lhs.getRelationNames(), rhs.getRelationNames());
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return Lists.concat(lhs.baseTables(), rhs.baseTables());
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(lhs, rhs);
    }


    protected static MergePhase buildMergePhaseForJoin(PlannerContext plannerContext,
                                                       ResultDescription resultDescription,
                                                       Collection<String> executionNodes) {
        List<Projection> projections = Collections.emptyList();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            projections = Collections.singletonList(ProjectionBuilder.limitAndOffsetOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        }

        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "join-merge",
            resultDescription.nodeIds().size(),
            1,
            executionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
    }
}
