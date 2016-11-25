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

import com.google.common.base.Optional;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.List;

class ReduceOnCollectorGroupByConsumer implements Consumer {

    private final Visitor visitor;

    ReduceOnCollectorGroupByConsumer(Functions functions) {
        visitor = new Visitor(functions);
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (!table.querySpec().groupBy().isPresent()) {
                return null;
            }
            DocTableRelation tableRelation = table.tableRelation();
            if (!GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                tableRelation, table.querySpec().where(), table.querySpec().groupBy().get())) {
                return null;
            }

            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return optimizedReduceOnCollectorGroupBy(table, tableRelation, context);
        }

        /**
         * grouping on doc tables by clustered column or primary keys, no distribution needed
         * only one aggregation step as the mappers (shards) have row-authority
         * <p>
         * produces:
         * <p>
         * SELECT:
         * CollectNode ( GroupProjection, [FilterProjection], [TopN] )
         * LocalMergeNode ( TopN )
         */
        private Plan optimizedReduceOnCollectorGroupBy(QueriedDocTable table, DocTableRelation tableRelation, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            Optional<List<Symbol>> optGroupBy = querySpec.groupBy();
            assert optGroupBy.isPresent() : "must have groupBy if optimizeReduceOnCollectorGroupBy is called";
            List<Symbol> groupKeys = optGroupBy.get();
            assert GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                tableRelation, querySpec.where(), groupKeys) : "not grouped by clustered column or primary keys";
            GroupByConsumer.validateGroupBySymbols(tableRelation, groupKeys);

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // mapper / collect
            List<Symbol> collectOutputs = new ArrayList<>(
                groupKeys.size() +
                splitPoints.aggregates().size());
            collectOutputs.addAll(groupKeys);
            collectOutputs.addAll(splitPoints.aggregates());

            table.tableRelation().validateOrderBy(querySpec.orderBy());

            List<Projection> projections = new ArrayList<>();
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.leaves(),
                groupKeys,
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.FINAL,
                RowGranularity.SHARD
            );
            projections.add(groupProjection);

            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                HavingClause having = havingClause.get();
                FilterProjection fp = ProjectionBuilder.filterProjection(collectOutputs, having);
                fp.requiredGranularity(RowGranularity.SHARD);
                projections.add(fp);
            }

            OrderBy orderBy = querySpec.orderBy().orNull();
            Limits limits = context.plannerContext().getLimits(querySpec);
            List<Symbol> qsOutputs = querySpec.outputs();
            projections.add(ProjectionBuilder.topNProjection(
                collectOutputs,
                orderBy,
                0, // no offset
                limits.limitAndOffset(),
                qsOutputs
            ));
            RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
                context.plannerContext(),
                table,
                splitPoints.leaves(),
                projections,
                table.relationId()
            );
            return new Collect(
                collectPhase,
                limits.finalLimit(),
                limits.offset(),
                qsOutputs.size(),
                limits.limitAndOffset(),
                PositionalOrderBy.of(orderBy, qsOutputs)
            );
        }
    }
}
