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

package io.crate.planner.consumer;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueryClause;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import java.util.Collection;

public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static final  class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec qs = relation.querySpec();
            if (!isSimpleSelect(qs, context)) {
                return null;
            }
            Planner.Context plannerContext = context.plannerContext();
            QueriedRelation subRelation = relation.subRelation();
            FetchMode parentFetchMode = context.fetchMode();
            if (parentFetchMode != FetchMode.NEVER) {
                context.setFetchMode(FetchMode.WITH_PROPAGATION);
            }
            Plan subPlan = plannerContext.planSubRelation(subRelation, context);
            context.setFetchMode(parentFetchMode);

            return finalizeQueriedSelectPlanWithoutFetch(qs, plannerContext, subRelation, subPlan);
        }
    }

    private static Plan finalizeQueriedSelectPlanWithoutFetch(QuerySpec qs,
                                                              Planner.Context plannerContext,
                                                              QueriedRelation subRelation,
                                                              Plan subPlan) {
        subPlan = Merge.ensureOnHandler(subPlan, plannerContext);
        applyFilter(subPlan, subRelation.fields(), qs.where());
        Limits limits = plannerContext.getLimits(qs);
        Projection projection = ProjectionBuilder.topNOrEval(
            subRelation.fields(),
            qs.orderBy(),
            limits.offset(),
            limits.finalLimit(),
            qs.outputs()
        );
        subPlan.addProjection(projection, TopN.NO_LIMIT, 0, null);
        return subPlan;
    }

    private static void applyFilter(Plan plan, Collection<? extends Symbol> filterInputs, QueryClause query) {
        if (query.hasQuery() || query.noMatch()) {
            FilterProjection filterProjection = ProjectionBuilder.filterProjection(filterInputs, query);
            plan.addProjection(filterProjection);
        }
    }

    private static boolean isSimpleSelect(QuerySpec querySpec, ConsumerContext context) {
        if (querySpec.hasAggregates() || !querySpec.groupBy().isEmpty()) {
            return false;
        }
        if (querySpec.where().hasVersions()) {
            context.validationException(new VersionInvalidException());
            return false;
        }
        return true;
    }
}
