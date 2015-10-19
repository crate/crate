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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.ValidationException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.fetch.FetchRequiredVisitor;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.*;


@Singleton
public class CrossJoinConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public CrossJoinConsumer(ClusterService clusterService,
                             AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(clusterService, analysisMetaData);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private static final Predicate<MultiSourceSelect.Source> DOC_TABLE_RELATION = new Predicate<MultiSourceSelect.Source>() {
            @Override
            public boolean apply(MultiSourceSelect.Source input) {
                AnalyzedRelation relation = input.relation();
                return relation instanceof DocTableRelation || relation instanceof QueriedDocTable;
            }
        };
        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            boolean hasDocTables = Iterables.any(statement.sources().values(), DOC_TABLE_RELATION);
            if (isUnsupportedStatement(statement, context, hasDocTables)) return null;
            if (statement.querySpec().where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }

            final Collection<QualifiedName> relationNames = getOrderedRelationNames(statement);

            // TODO: replace references with docIds.. and add fetch projection

            List<QueriedTableRelation> queriedTables = new ArrayList<>(relationNames.size());
            for (QualifiedName relationName : relationNames) {
                MultiSourceSelect.Source source = statement.sources().get(relationName);
                QueriedTableRelation queriedTable;
                try {
                    queriedTable = SubRelationConverter.INSTANCE.process(source.relation(), source);
                } catch (ValidationException e) {
                    context.validationException(e);
                    return null;
                }
                queriedTables.add(queriedTable);
            }

            WhereClause where = statement.querySpec().where();
            boolean filterNeeded = where.hasQuery() && !(where.query() instanceof Literal);
            boolean isDistributed = hasDocTables && filterNeeded;

            // for nested loops we are fine to remove pushed down orders
            OrderBy remainingOrderBy = statement.remainingOrderBy();
            OrderBy orderByBeforeSplit = statement.querySpec().orderBy().orNull();

            // replace all the fields in the root query spec
            RelationSplitter.replaceFields(queriedTables, statement.querySpec());

            //TODO: queriedTable.normalize(analysisMetaData);

            QueriedTableRelation<?> left = queriedTables.get(0);
            QueriedTableRelation<?> right = queriedTables.get(1);

            if (filterNeeded || remainingOrderBy != null) {
                for (QueriedTableRelation queriedTable : queriedTables) {
                    queriedTable.querySpec().limit(null);
                    queriedTable.querySpec().offset(TopN.NO_OFFSET);
                }
            }

            if (!filterNeeded && statement.querySpec().limit().isPresent()) {
                context.requiredPageSize(statement.querySpec().limit().get() + statement.querySpec().offset());
            }

            // this normalization is required to replace fields of the table relations
            left.normalize(analysisMetaData);
            right.normalize(analysisMetaData);

            // plan the subRelations
            PlannedAnalyzedRelation leftPlan = context.plannerContext().planSubRelation(left, context);
            PlannedAnalyzedRelation rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            Set<String> localExecutionNodes = ImmutableSet.of(clusterService.localNode().id());
            Collection<String> nlExecutionNodes = localExecutionNodes;


            MergePhase leftMerge = null;
            MergePhase rightMerge = null;
            if (isDistributed) {
                leftPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                nlExecutionNodes = leftPlan.resultPhase().executionNodes();
            } else {
                leftMerge = mergePhase(
                        context,
                        nlExecutionNodes,
                        leftPlan.resultPhase(),
                        left.querySpec().orderBy().orNull(),
                        left.querySpec().outputs(),
                        false);
            }
            if (nlExecutionNodes.size() == 1
                    && nlExecutionNodes.equals(rightPlan.resultPhase().executionNodes())) {
                // if the left and the right plan are executed on the same single node the mergePhase
                // should be omitted. This is the case if the left and right table have only one shards which
                // are on the same node
                rightPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            } else {
                rightMerge = mergePhase(
                        context,
                        nlExecutionNodes,
                        rightPlan.resultPhase(),
                        right.querySpec().orderBy().orNull(),
                        right.querySpec().outputs(),
                        isDistributed);
                rightPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }

            List<Projection> projections = new ArrayList<>();
            List<Field> inputs = concatFields(left, right);
            if (filterNeeded) {
                FilterProjection filterProjection = ProjectionBuilder.filterProjection(inputs, where.query());
                projections.add(filterProjection);
            }

            List<Symbol> postNLOutputs = Lists.newArrayList(statement.querySpec().outputs());
            if (orderByBeforeSplit != null && isDistributed) {
                for (Symbol symbol : orderByBeforeSplit.orderBySymbols()) {
                    if (postNLOutputs.indexOf(symbol) == -1) {
                        postNLOutputs.add(symbol);
                    }
                }
            }

            int topNLimit = statement.querySpec().limit().or(Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN = ProjectionBuilder.topNProjection(
                    inputs,
                    remainingOrderBy,
                    isDistributed ? 0 : statement.querySpec().offset(),
                    isDistributed ? topNLimit + statement.querySpec().offset() : topNLimit,
                    postNLOutputs
            );
            projections.add(topN);

            NestedLoopPhase nl = new NestedLoopPhase(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    isDistributed ? "distributed-nested-loop" : "nested-loop",
                    projections,
                    leftMerge,
                    rightMerge,
                    nlExecutionNodes
            );
            if (isDistributed) {
                nl.distributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }
            MergePhase localMergePhase = null;
            if (isDistributed && context.rootRelation() == statement) {
                localMergePhase = mergePhase(context, localExecutionNodes, nl, orderByBeforeSplit, postNLOutputs, true);
                assert localMergePhase != null : "local merge phase must not be null";
                TopNProjection finalTopN = ProjectionBuilder.topNProjection(
                        postNLOutputs,
                        orderByBeforeSplit,
                        statement.querySpec().offset(),
                        statement.querySpec().limit().or(Constants.DEFAULT_SELECT_LIMIT),
                        statement.querySpec().outputs()
                );
                localMergePhase.addProjection(finalTopN);
            }
            return new NestedLoop(nl, leftPlan, rightPlan, true, localMergePhase);
        }

        private static List<Field> concatFields(QueriedTableRelation<?> left, QueriedTableRelation<?> right) {
            List<Field> inputs = new ArrayList<>(left.fields().size() + right.fields().size());
            inputs.addAll(left.fields());
            inputs.addAll(right.fields());
            return inputs;
        }

        private MergePhase mergePhase(ConsumerContext context,
                                      Collection<String> executionNodes,
                                      UpstreamPhase upstreamPhase,
                                      @Nullable OrderBy orderBy,
                                      List<Symbol> previousOutputs,
                                      boolean isDistributed) {
            assert !upstreamPhase.executionNodes().isEmpty() : "upstreamPhase must be executed somewhere";
            if (!isDistributed && upstreamPhase.executionNodes().equals(executionNodes)) {
                // if the nested loop is on the same node we don't need a mergePhase to receive requests
                // but can access the RowReceiver of the nestedLoop directly
                return null;
            }

            MergePhase mergePhase;
            if (orderBy != null) {
                mergePhase = MergePhase.sortedMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        orderBy,
                        previousOutputs,
                        orderBy.orderBySymbols(),
                        ImmutableList.<Projection>of(),
                        upstreamPhase.executionNodes().size(),
                        Symbols.extractTypes(previousOutputs)
                );
            } else {
                mergePhase = MergePhase.localMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        ImmutableList.<Projection>of(),
                        upstreamPhase.executionNodes().size(),
                        Symbols.extractTypes(previousOutputs)
                );
            }
            mergePhase.executionNodes(executionNodes);
            return mergePhase;
        }

        /**
         * returns a map with the relation as keys and the values are their order in occurrence in the order by clause.
         * <p/>
         * e.g. select * from t1, t2, t3 order by t2.x, t3.y
         * <p/>
         * returns: {
         * t2: 0                 (first)
         * t3: 1                 (second)
         * }
         */
        private Collection<QualifiedName> getOrderedRelationNames(MultiSourceSelect statement) {
            if (!statement.querySpec().orderBy().isPresent()) {
                return statement.sources().keySet();
            }
            Optional<OrderBy> orderBy = statement.querySpec().orderBy();
            final List<QualifiedName> orderByOrder = new ArrayList<>(statement.sources().size());
            for (Symbol orderBySymbol : orderBy.get().orderBySymbols()) {
                for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : statement.sources().entrySet()) {
                    Optional<OrderBy> subOrderBy = entry.getValue().querySpec().orderBy();
                    if (!subOrderBy.isPresent() || orderByOrder.contains(entry.getKey())) {
                        continue;
                    }
                    if (orderBySymbol.equals(subOrderBy.get().orderBySymbols().get(0))) {
                        orderByOrder.add(entry.getKey());
                        break;
                    }
                }
            }
            if (orderByOrder.size() < statement.sources().size()) {
                Iterator<QualifiedName> iter = statement.sources().keySet().iterator();
                while (orderByOrder.size() < statement.sources().size()) {
                    QualifiedName qn = iter.next();
                    if (!orderByOrder.contains(qn)) {
                        orderByOrder.add(qn);
                    }
                }
            }
            return orderByOrder;
        }

        private boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context, boolean hasDocTables) {
            if (statement.sources().size() != 2) {
                context.validationException(new ValidationException("Joining more than 2 relations is not supported"));
                return true;
            }

            if (statement.querySpec().groupBy().isPresent()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return true;
            }
            if (hasDocTables && hasOutputsToFetch(statement.querySpec())) {
                context.validationException(new ValidationException("Only fields that are used in ORDER BY can be selected within a CROSS JOIN"));
                return true;
            }
            return false;
        }

        private boolean hasOutputsToFetch(QuerySpec querySpec) {
            FetchRequiredVisitor.Context ctx = new FetchRequiredVisitor.Context(querySpec.orderBy().orNull());
            return FetchRequiredVisitor.INSTANCE.process(querySpec.outputs(), ctx);
        }
    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<MultiSourceSelect.Source, QueriedTableRelation> {

        static final SubRelationConverter INSTANCE = new SubRelationConverter();

        @Override
        public QueriedTableRelation visitTableRelation(TableRelation tableRelation,
                                                       MultiSourceSelect.Source source) {
            return new QueriedTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedTableRelation visitDocTableRelation(DocTableRelation tableRelation,
                                                          MultiSourceSelect.Source source) {
            return new QueriedDocTable(tableRelation, source.querySpec());
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation,
                                                             MultiSourceSelect.Source source) {
            throw new ValidationException("CROSS JOIN with sub queries is not supported");
        }
    }

}
