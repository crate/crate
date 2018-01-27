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

package io.crate.planner.operators;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.util.set.Sets;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.crate.planner.operators.Limit.limitAndOffset;
import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

public class Join extends TwoInputPlan {

    final JoinType joinType;

    @Nullable
    final Symbol joinCondition;
    private final boolean isFiltered;
    private final boolean hasOuterJoins;
    private final AnalyzedRelation leftRelation;

    static Builder createNodes(MultiSourceSelect mss, WhereClause where, SubqueryPlanner subqueryPlanner) {
        return (tableStats, usedColsByParent) -> {

            LinkedHashMap<Set<QualifiedName>, JoinPair> joinPairs = new LinkedHashMap<>();
            for (JoinPair joinPair : mss.joinPairs()) {
                if (joinPair.condition() == null) {
                    continue;
                }
                JoinPair prevPair = joinPairs.put(Sets.newHashSet(joinPair.left(), joinPair.right()), joinPair);
                if (prevPair != null) {
                    throw new IllegalStateException("joinPairs contains duplicate: " + joinPair + " matches " + prevPair);
                }
            }
            final boolean hasOuterJoins = joinPairs.values().stream().anyMatch(p -> p.joinType().isOuter());
            Map<Set<QualifiedName>, Symbol> queryParts = getQueryParts(where);

            Collection<QualifiedName> orderedRelationNames;
            if (mss.sources().size() > 2) {
                orderedRelationNames = JoinOrdering.getOrderedRelationNames(
                    mss.sources().keySet(),
                    joinPairs.keySet(),
                    queryParts.keySet()
                );
            } else {
                orderedRelationNames = mss.sources().keySet();
            }

            Iterator<QualifiedName> it = orderedRelationNames.iterator();

            final QualifiedName lhsName = it.next();
            final QualifiedName rhsName = it.next();
            QueriedRelation lhs = (QueriedRelation) mss.sources().get(lhsName);
            QueriedRelation rhs = (QueriedRelation) mss.sources().get(rhsName);
            Set<QualifiedName> joinNames = new HashSet<>();
            joinNames.add(lhsName);
            joinNames.add(rhsName);

            JoinPair joinLhsRhs = joinPairs.remove(joinNames);
            final JoinType joinType;
            final Symbol joinCondition;
            if (joinLhsRhs == null) {
                joinType = JoinType.CROSS;
                joinCondition = null;
            } else {
                joinType = maybeInvertPair(rhsName, joinLhsRhs);
                joinCondition = joinLhsRhs.condition();
            }

            Set<Symbol> usedFromLeft = new HashSet<>();
            Set<Symbol> usedFromRight = new HashSet<>();
            for (JoinPair joinPair : mss.joinPairs()) {
                addColumnsFrom(joinPair.condition(), usedFromLeft::add, lhs);
                addColumnsFrom(joinPair.condition(), usedFromRight::add, rhs);
            }
            addColumnsFrom(where.query(), usedFromLeft::add, lhs);
            addColumnsFrom(where.query(), usedFromRight::add, rhs);

            addColumnsFrom(usedColsByParent, usedFromLeft::add, lhs);
            addColumnsFrom(usedColsByParent, usedFromRight::add, rhs);

            // use NEVER_CLEAR as fetchMode to prevent intermediate fetches
            // This is necessary; because due to how the fetch-reader-allocation works it's not possible to
            // have more than 1 fetchProjection within a single execution
            LogicalPlan lhsPlan = LogicalPlanner.plan(lhs, FetchMode.NEVER_CLEAR, subqueryPlanner, false).build(tableStats, usedFromLeft);
            LogicalPlan rhsPlan = LogicalPlanner.plan(rhs, FetchMode.NEVER_CLEAR, subqueryPlanner, false).build(tableStats, usedFromRight);
            Symbol query = removeParts(queryParts, lhsName, rhsName);
            LogicalPlan join = new Join(
                lhsPlan,
                rhsPlan,
                joinType,
                joinCondition,
                query != null && !(query instanceof Literal),
                hasOuterJoins,
                lhs);

            join = Filter.create(join, query);
            while (it.hasNext()) {
                QueriedRelation nextRel = (QueriedRelation) mss.sources().get(it.next());
                join = joinWithNext(
                    tableStats,
                    join,
                    nextRel,
                    usedColsByParent,
                    joinNames,
                    joinPairs,
                    queryParts,
                    subqueryPlanner,
                    hasOuterJoins,
                    lhs);
                joinNames.add(nextRel.getQualifiedName());
            }
            assert queryParts.isEmpty() : "Must've applied all queryParts";
            assert joinPairs.isEmpty() : "Must've applied all joinPairs";

            return join;
        };
    }

    private static JoinType maybeInvertPair(QualifiedName rhsName, JoinPair pair) {
        // A matching joinPair for two relations is retrieved using pairByQualifiedNames.remove(setOf(a, b))
        // This returns a pair for both cases: (a ⋈ b) and (b ⋈ a) -> invert joinType to execute correct join
        // Note that this can only happen if a re-ordering optimization happened, otherwise the joinPair would always
        // be in the correct format.
        if (pair.right().equals(rhsName)) {
            return pair.joinType();
        }
        return pair.joinType().invert();
    }

    private static LogicalPlan joinWithNext(TableStats tableStats,
                                            LogicalPlan source,
                                            QueriedRelation nextRel,
                                            Set<Symbol> usedColumns,
                                            Set<QualifiedName> joinNames,
                                            Map<Set<QualifiedName>, JoinPair> joinPairs,
                                            Map<Set<QualifiedName>, Symbol> queryParts,
                                            SubqueryPlanner subqueryPlanner,
                                            boolean hasOuterJoins,
                                            AnalyzedRelation leftRelation) {
        QualifiedName nextName = nextRel.getQualifiedName();

        Set<Symbol> usedFromNext = new HashSet<>();
        Consumer<Symbol> addToUsedColumns = usedFromNext::add;
        JoinPair joinPair = removeMatch(joinPairs, joinNames, nextName);
        final JoinType type;
        final Symbol condition;
        if (joinPair == null) {
            type = JoinType.CROSS;
            condition = null;
        } else {
            type = maybeInvertPair(nextName, joinPair);
            condition = joinPair.condition();
            addColumnsFrom(condition, addToUsedColumns, nextRel);
        }
        for (JoinPair pair : joinPairs.values()) {
            addColumnsFrom(pair.condition(), addToUsedColumns, nextRel);
        }
        for (Symbol queryPart : queryParts.values()) {
            addColumnsFrom(queryPart, addToUsedColumns, nextRel);
        }
        addColumnsFrom(usedColumns, addToUsedColumns, nextRel);

        LogicalPlan nextPlan = LogicalPlanner.plan(nextRel, FetchMode.NEVER_CLEAR, subqueryPlanner, false).build(tableStats, usedFromNext);

        Symbol query = AndOperator.join(
            Stream.of(
                removeMatch(queryParts, joinNames, nextName),
                queryParts.remove(Collections.singleton(nextName)))
                .filter(Objects::nonNull).iterator()
        );
        return Filter.create(
            new Join(
                source,
                nextPlan,
                type,
                condition,
                query != null && !(query instanceof Literal),
                hasOuterJoins,
                leftRelation),
            query
        );
    }

    @Nullable
    private static Symbol removeParts(Map<Set<QualifiedName>, Symbol> queryParts, QualifiedName lhsName, QualifiedName rhsName) {
        // query parts can affect a single relation without being pushed down in the outer-join case
        Symbol left = queryParts.remove(Collections.singleton(lhsName));
        Symbol right = queryParts.remove(Collections.singleton(rhsName));
        Symbol both = queryParts.remove(Sets.newHashSet(lhsName, rhsName));
        return AndOperator.join(
            Stream.of(left, right, both).filter(Objects::nonNull).iterator()
        );
    }

    @Nullable
    private static <V> V removeMatch(Map<Set<QualifiedName>, V> valuesByNames, Set<QualifiedName> names, QualifiedName nextName) {
        for (QualifiedName name : names) {
            V v = valuesByNames.remove(Sets.newHashSet(name, nextName));
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static void addColumnsFrom(Iterable<? extends Symbol> symbols,
                                       Consumer<? super Symbol> consumer,
                                       QueriedRelation rel) {

        for (Symbol symbol : symbols) {
            addColumnsFrom(symbol, consumer, rel);
        }
    }

    private static void addColumnsFrom(@Nullable Symbol symbol, Consumer<? super Symbol> consumer, QueriedRelation rel) {
        if (symbol == null) {
            return;
        }
        FieldsVisitor.visitFields(symbol, f -> {
            if (f.relation().getQualifiedName().equals(rel.getQualifiedName())) {
                consumer.accept(rel.querySpec().outputs().get(f.index()));
            }
        });
    }

    private static Map<Set<QualifiedName>,Symbol> getQueryParts(WhereClause where) {
        if (where.hasQuery()) {
            return QuerySplitter.split(where.query());
        }
        return Collections.emptyMap();
    }

    private Join(LogicalPlan lhs,
                 LogicalPlan rhs,
                 JoinType joinType,
                 @Nullable Symbol joinCondition,
                 boolean isFiltered,
                 boolean hasOuterJoins,
                 AnalyzedRelation leftRelation) {
        super(lhs, rhs, new ArrayList<>());
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.isFiltered = isFiltered;
        if (joinType == JoinType.SEMI) {
            this.outputs.addAll(lhs.outputs());
        } else {
            this.outputs.addAll(lhs.outputs());
            this.outputs.addAll(rhs.outputs());
        }
        this.hasOuterJoins = hasOuterJoins;
        this.leftRelation = leftRelation;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {
        /*
         * isDistributed/filterNeeded doesn't consider the joinCondition.
         * This means joins with implicit syntax result in a different plan than joins using explicit syntax.
         * This was unintentional but we'll keep it that way (for now) as a distributed plan can be significantly slower
         * (depending on the number of rows that are filtered)
         * and we don't want to introduce a performance regression.
         *
         * We may at some point add some kind of session-settings to override this behaviour or otherwise
         * come up with a better heuristic.
         */
        Integer childPageSizeHint = !isFiltered && joinCondition == null && limit != TopN.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryValues);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryValues);


        boolean hasDocTables = baseTables.stream().anyMatch(r -> r instanceof DocTableRelation);
        JoinType joinType = this.joinType;
        boolean isDistributed = hasDocTables && isFiltered && !joinType.isOuter();

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();
        isDistributed = isDistributed &&
                        (!leftResultDesc.nodeIds().isEmpty() && !rightResultDesc.nodeIds().isEmpty());
        boolean switchTables = false;
        if (isDistributed && joinType.supportsInversion() && lhs.numExpectedRows() < rhs.numExpectedRows()) {
            // temporarily switch plans and relations to apply broadcasting logic
            // to smaller side (which is always the right side).
            switchTables = true;
            ExecutionPlan tmpExecutionPlan = left;
            left = right;
            right = tmpExecutionPlan;

            leftResultDesc = left.resultDescription();
            rightResultDesc = right.resultDescription();
        }
        Collection<String> nlExecutionNodes = ImmutableSet.of(plannerContext.handlerNode());

        MergePhase leftMerge = null;
        MergePhase rightMerge = null;
        if (isDistributed && !leftResultDesc.hasRemainingLimitOrOffset()) {
            left.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            nlExecutionNodes = leftResultDesc.nodeIds();
        } else {
            left.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
                leftMerge = buildMergePhase(plannerContext, leftResultDesc, nlExecutionNodes);
            }
        }
        if (nlExecutionNodes.size() == 1
            && nlExecutionNodes.equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // if the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            right.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            if (isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, isDistributed)) {
                rightMerge = buildMergePhase(plannerContext, rightResultDesc, nlExecutionNodes);
            }
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        }

        if (switchTables) {
            ExecutionPlan tmpExecutionPlan = left;
            left = right;
            right = tmpExecutionPlan;
            MergePhase tmp = leftMerge;
            leftMerge = rightMerge;
            rightMerge = tmp;
        }
        Symbol joinInput = null;
        if (joinCondition != null) {
            joinInput = InputColumns.create(joinCondition, Lists2.concat(lhs.outputs(), rhs.outputs()));
        }

        NestedLoopPhase nlPhase = new NestedLoopPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            isDistributed ? "distributed-nested-loop" : "nested-loop",
            // NestedLoopPhase ctor want's at least one projection
            Collections.singletonList(new EvalProjection(InputColumn.fromSymbols(outputs))),
            leftMerge,
            rightMerge,
            nlExecutionNodes,
            joinType,
            joinInput,
            lhs.outputs().size(),
            rhs.outputs().size()
        );
        return new NestedLoop(
            nlPhase,
            left,
            right,
            TopN.NO_LIMIT,
            0,
            TopN.NO_LIMIT,
            outputs.size(),
            null
        );
    }

    private static boolean isMergePhaseNeeded(Collection<String> executionNodes,
                                              ResultDescription resultDescription,
                                              boolean isDistributed) {
        return isDistributed ||
               resultDescription.hasRemainingLimitOrOffset() ||
               !resultDescription.nodeIds().equals(executionNodes);
    }

    private static MergePhase buildMergePhase(PlannerContext plannerContext,
                                              ResultDescription resultDescription,
                                              Collection<String> nlExecutionNodes) {
        List<Projection> projections = Collections.emptyList();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            projections = Collections.singletonList(ProjectionBuilder.topNOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        }

        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "nl-merge",
            resultDescription.nodeIds().size(),
            1,
            nlExecutionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
    }

    @Override
    protected LogicalPlan updateSources(LogicalPlan newLeftSource, LogicalPlan newRightSource) {
        return new Join(newLeftSource, newRightSource, joinType, joinCondition, isFiltered, hasOuterJoins, leftRelation);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        HashMap<LogicalPlan, SelectSymbol> deps = new HashMap<>();
        deps.putAll(lhs.dependencies());
        deps.putAll(rhs.dependencies());
        return deps;
    }

    @Override
    public long numExpectedRows() {
        if (joinType == JoinType.CROSS) {
            return lhs.numExpectedRows() * rhs.numExpectedRows();
        } else {
            // We don't have any cardinality estimates, so just take the bigger table
            return Math.max(lhs.numExpectedRows(), rhs.numExpectedRows());
        }
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitJoin(this, context);
    }

    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown) {
        if (pushDown instanceof Order) {
            /* Move the orderBy expression to the sub-relation if possible.
             *
             * This is possible because a nested loop preserves the ordering of the input-relation
             * IF:
             *   - the order by expressions only operate using fields from a single relation
             *   - that relation happens to be on the left-side of the join
             *   - there is no outer join involved in the whole join (outer joins may create null rows - breaking the ordering)
             */
            if (hasOuterJoins == false) {
                Set<AnalyzedRelation> relationsInOrderBy =
                    Collections.newSetFromMap(new IdentityHashMap<AnalyzedRelation, Boolean>());
                Consumer<Field> gatherRelations = f -> relationsInOrderBy.add(f.relation());

                OrderBy orderBy = ((Order) pushDown).orderBy;
                for (Symbol orderExpr : orderBy.orderBySymbols()) {
                    FieldsVisitor.visitFields(orderExpr, gatherRelations);
                }
                if (relationsInOrderBy.size() == 1) {
                    AnalyzedRelation relationInOrderBy = relationsInOrderBy.iterator().next();
                    if (relationInOrderBy == leftRelation) {
                        LogicalPlan newLhs = lhs.tryOptimize(pushDown);
                        if (newLhs != null) {
                            return updateSources(newLhs, rhs);
                        }
                    }
                }
            }
        }
        return super.tryOptimize(pushDown);
    }
}
