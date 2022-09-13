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

package io.crate.execution.dsl.projection;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;

public class CorrelatedJoinProjection extends Projection {

    private final List<Symbol> outputs;
    private final LogicalPlan subQueryPlan;
    private final SelectSymbol correlatedSubQuery;
    private final PlannerContext plannerContext;
    private final SubQueryResults subQueryResults;
    private final Row params;
    private final List<Symbol> inputs;
    private final DependencyCarrier executor;

    public CorrelatedJoinProjection(DependencyCarrier executor,
                                    LogicalPlan subQueryPlan,
                                    SelectSymbol correlatedSubQuery,
                                    PlannerContext plannerContext,
                                    SubQueryResults subQueryResults,
                                    Row params,
                                    List<Symbol> inputs,
                                    List<Symbol> outputs) {
        this.executor = executor;
        this.subQueryPlan = subQueryPlan;
        this.correlatedSubQuery = correlatedSubQuery;
        this.plannerContext = plannerContext;
        this.subQueryResults = subQueryResults;
        this.params = params;
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public LogicalPlan subQueryPlan() {
        return subQueryPlan;
    }

    public SelectSymbol correlatedSubQuery() {
        return correlatedSubQuery;
    }

    public PlannerContext plannerContext() {
        return plannerContext;
    }

    public SubQueryResults subQueryResults() {
        return subQueryResults;
    }

    public Row params() {
        return params;
    }

    public List<Symbol> inputPlanOutputs() {
        return inputs;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.CORRELATED_JOIN;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitCorrelatedJoin(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream correlated join projection");
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    public DependencyCarrier executor() {
        return executor;
    }
}
