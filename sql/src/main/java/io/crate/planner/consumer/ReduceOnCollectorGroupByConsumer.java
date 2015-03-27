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

import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.NonDistributedGroupBy;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class ReduceOnCollectorGroupByConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        Context ctx = new Context(context);
        context.rootRelation(VISITOR.process(context.rootRelation(), ctx));
        return ctx.result;
    }

    private static class Context {
        ConsumerContext consumerContext;
        boolean result = false;

        public Context(ConsumerContext context) {
            this.consumerContext = context;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            if (table.querySpec().groupBy() == null) {
                return table;
            }

            if (!GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(table.tableRelation(), table.querySpec().groupBy())) {
                return table;
            }

            // no row authority on shards for partitioned tables when grouping by routing column
            // could span multiple partitions (clustered by is not part of primary key)
            // but when only one partition is hit, or we have primary keys we can use optimized grouping
            if (!clusteredByIsPartOfPrimaryKey(table.tableRelation().tableInfo())
                    && table.tableRelation().tableInfo().isPartitioned()
                    && (table.querySpec().where().partitions().size() != 1)) {
                return table;
            }


            if (table.querySpec().where().hasVersions()) {
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }
            context.result = true;
            return optimizedReduceOnCollectorGroupBy(table, table.tableRelation(), context.consumerContext);
        }

        private boolean clusteredByIsPartOfPrimaryKey(TableInfo tableInfo) {
            return tableInfo.primaryKey().contains(tableInfo.clusteredBy());
        }

        @Override
        public AnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, Context context) {
            InsertFromSubQueryConsumer.planInnerRelation(insertFromSubQueryAnalyzedStatement, context, this);
            return insertFromSubQueryAnalyzedStatement;
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        /**
         * grouping on doc tables by clustered column or primary keys, no distribution needed
         * only one aggregation step as the mappers (shards) have row-authority
         *
         * produces:
         *
         * SELECT:
         * CollectNode ( GroupProjection, [FilterProjection], [TopN] )
         * LocalMergeNode ( TopN )
         */
        private AnalyzedRelation optimizedReduceOnCollectorGroupBy(QueriedTable table, TableRelation tableRelation, ConsumerContext context) {
            assert GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(tableRelation, table.querySpec().groupBy()) : "not grouped by clustered column or primary keys";
            TableInfo tableInfo = tableRelation.tableInfo();
            GroupByConsumer.validateGroupBySymbols(tableRelation, table.querySpec().groupBy());
            List<Symbol> groupBy = table.querySpec().groupBy();

            boolean ignoreSorting = context.rootRelation() != table
                    && table.querySpec().limit() == null
                    && table.querySpec().offset() == TopN.NO_OFFSET;

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(table.querySpec());
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // mapper / collect
            List<Symbol> collectOutputs = new ArrayList<>(
                    groupBy.size() +
                            splitPoints.aggregates().size());
            collectOutputs.addAll(groupBy);
            collectOutputs.addAll(splitPoints.aggregates());

            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null) {
                table.tableRelation().validateOrderBy(orderBy);
            }

            List<Projection> projections = new ArrayList<>();
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                    splitPoints.leaves(),
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.ITER,
                    Aggregation.Step.FINAL
            );
            groupProjection.setRequiredGranularity(RowGranularity.SHARD);
            projections.add(groupProjection);

            HavingClause havingClause = table.querySpec().having();
            if (havingClause != null) {
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table);
                } else if (havingClause.hasQuery()) {
                    FilterProjection fp = projectionBuilder.filterProjection(
                            collectOutputs,
                            havingClause.query()
                    );
                    fp.requiredGranularity(RowGranularity.SHARD);
                    projections.add(fp);
                }
            }
            // mapper / collect
            // use topN on collector if needed
            boolean outputsMatch = table.querySpec().outputs().size() == collectOutputs.size() &&
                    collectOutputs.containsAll(table.querySpec().outputs());
            boolean collectorTopN = table.querySpec().limit() != null || table.querySpec().offset() > 0 || !outputsMatch;

            if (collectorTopN) {
                projections.add(projectionBuilder.topNProjection(
                        collectOutputs,
                        orderBy,
                        0, // no offset
                        firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset(),
                        table.querySpec().outputs()
                ));
            }

            CollectNode collectNode = PlanNodeBuilder.collect(
                    tableInfo,
                    context.plannerContext(),
                    table.querySpec().where(),
                    splitPoints.leaves(),
                    ImmutableList.copyOf(projections)
            );

            // handler
            List<Projection> handlerProjections = new ArrayList<>();
            MergeNode localMergeNode;
            if (!ignoreSorting && collectorTopN && orderBy != null && orderBy.isSorted()) {
                // handler receives sorted results from collect nodes
                // we can do the sorting with a sorting bucket merger
                localMergeNode = PlanNodeBuilder.sortedLocalMerge(
                        handlerProjections, orderBy, table.querySpec().outputs(), null,
                        collectNode, context.plannerContext());
                handlerProjections.add(
                        projectionBuilder.topNProjection(
                                table.querySpec().outputs(),
                                null, // omit order by
                                table.querySpec().offset(),
                                firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                                table.querySpec().outputs()
                        )
                );
            } else {
                handlerProjections.add(
                        projectionBuilder.topNProjection(
                                collectorTopN ? table.querySpec().outputs() : collectOutputs,
                                orderBy,
                                table.querySpec().offset(),
                                firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                                table.querySpec().outputs()
                        )
                );
                // fallback - unsorted local merge
                localMergeNode = PlanNodeBuilder.localMerge(handlerProjections, collectNode,
                        context.plannerContext());
            }
            return new NonDistributedGroupBy(collectNode, localMergeNode);
        }


    }
}
