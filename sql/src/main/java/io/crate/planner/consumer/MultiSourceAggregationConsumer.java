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

package io.crate.planner.consumer;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

class MultiSourceAggregationConsumer implements Consumer {

    private final Visitor visitor;

    MultiSourceAggregationConsumer(ProjectionBuilder projectionBuilder) {
        visitor = new Visitor(projectionBuilder);
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {


        private final ProjectionBuilder projectionBuilder;

        public Visitor(ProjectionBuilder projectionBuilder) {
            this.projectionBuilder = projectionBuilder;
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, ConsumerContext context) {
            QuerySpec qs = multiSourceSelect.querySpec();
            if (!qs.hasAggregates() || qs.groupBy().isPresent()) {
                return null;
            }
            qs = qs.copyAndReplace(i -> i); // copy because MSS planning mutates symbols
            SplitPoints splitPoints = SplitPoints.create(qs);
            removeAggregationsAndLimitsFromMSS(multiSourceSelect, splitPoints);
            Planner.Context plannerContext = context.plannerContext();
            context.setFetchMode(FetchMode.NEVER);
            Plan plan = plannerContext.planSubRelation(multiSourceSelect, context);

            // whereClause is already handled within the plan, no need to add additional FilterProjection via addAggregations
            qs.where(WhereClause.MATCH_ALL);
            return GlobalAggregateConsumer.addAggregations(
                qs, splitPoints.toCollect(), projectionBuilder, splitPoints, plannerContext, plan);
        }
    }

    private static void removeAggregationsAndLimitsFromMSS(MultiSourceSelect mss, SplitPoints splitPoints) {
        QuerySpec querySpec = mss.querySpec();
        List<Symbol> outputs = new ArrayList<>(splitPoints.toCollect());
        querySpec.outputs(outputs);
        querySpec.hasAggregates(false);
        // Limit & offset must be applied after the aggregation, so remove it from mss and sources.
        // OrderBy can be ignored because it's also applied after aggregation but there is always only 1 row so it
        // wouldn't have any effect.
        removeLimitOffsetAndOrder(querySpec);
        for (AnalyzedRelation relation : mss.sources().values()) {
            removeLimitOffsetAndOrder(((QueriedRelation) relation).querySpec());
        }

        // need to change the types on the fields of the MSS to match the new outputs
        ListIterator<Field> fieldsIt = mss.fields().listIterator();
        Iterator<Function> outputsIt = splitPoints.aggregates().iterator();
        while (fieldsIt.hasNext()) {
            Field field = fieldsIt.next();
            assert outputsIt.hasNext() : "too less collected aggregations, size must equal the outputs size";
            Symbol output = outputsIt.next();
            fieldsIt.set(new Field(field.relation(), field.path(), output.valueType()));
        }
    }

    private static void removeLimitOffsetAndOrder(QuerySpec querySpec) {
        if (querySpec.limit().isPresent()) {
            querySpec.limit(Optional.empty());
        }
        if (querySpec.offset().isPresent()) {
            querySpec.offset(Optional.empty());
        }
        if (querySpec.orderBy().isPresent()) {
            querySpec.orderBy(null);
        }
    }
}
