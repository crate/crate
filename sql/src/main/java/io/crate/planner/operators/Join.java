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

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ResultDescription;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.util.set.Sets;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

public class Join implements LogicalPlan {

    final LogicalPlan lhs;
    final LogicalPlan rhs;
    final List<Symbol> outputs;
    final JoinType joinType;

    @Nullable
    private final Symbol joinCondition;

    private final HashMap<Symbol, Symbol> expressionMapping;

    private final ArrayList<AbstractTableRelation> baseTables;

    static Builder createNodes(MultiSourceSelect mss, WhereClause where) {
        return usedColsByParent -> {
            Iterator<AnalyzedRelation> it = mss.sources().values().iterator();

            QueriedRelation lhs = (QueriedRelation) it.next();
            QueriedRelation rhs = (QueriedRelation) it.next();
            final QualifiedName lhsName = lhs.getQualifiedName();
            final QualifiedName rhsName = rhs.getQualifiedName();
            Set<QualifiedName> joinNames = new HashSet<>();
            joinNames.add(lhsName);
            joinNames.add(rhsName);

            Map<Set<QualifiedName>, JoinPair> joinPairs = mss.joinPairs()
                .stream()
                .collect(Collectors.toMap(p -> Sets.newHashSet(p.left(), p.right()), p -> p));

            JoinPair joinLhsRhs = joinPairs.remove(joinNames);

            Set<Symbol> usedFromLeft = new HashSet<>();
            Set<Symbol> usedFromRight = new HashSet<>();
            final JoinType joinType;
            final Symbol joinCondition;
            if (joinLhsRhs == null) {
                joinType = JoinType.CROSS;
                joinCondition = null;
            } else {
                joinType = joinLhsRhs.joinType();
                joinCondition = joinLhsRhs.condition();
            }

            for (JoinPair joinPair : mss.joinPairs()) {
                addColumnsFrom(joinPair.condition(), usedFromLeft::add, lhs);
                addColumnsFrom(joinPair.condition(), usedFromRight::add, rhs);
            }
            addColumnsFrom(where.query(), usedFromLeft::add, lhs);
            addColumnsFrom(where.query(), usedFromRight::add, rhs);

            addColumnsFrom(usedColsByParent, usedFromLeft::add, lhs);
            addColumnsFrom(usedColsByParent, usedFromRight::add, rhs);

            LogicalPlan lhsPlan = LogicalPlanner.plan(lhs, FetchMode.NEVER, false).build(usedFromLeft);
            LogicalPlan rhsPlan = LogicalPlanner.plan(rhs, FetchMode.NEVER, false).build(usedFromRight);
            LogicalPlan join = new Join(lhsPlan, rhsPlan, joinType, joinCondition);

            Map<Set<QualifiedName>, Symbol> queryParts = getQueryParts(where);
            Symbol query = removeParts(queryParts, lhsName, rhsName);
            join = Filter.create(join, query);
            while (it.hasNext()) {
                QueriedRelation nextRel = (QueriedRelation) it.next();
                join = joinWithNext(join, nextRel, usedColsByParent, joinNames, joinPairs, queryParts);
                joinNames.add(nextRel.getQualifiedName());
            }
            return join;
        };
    }

    private static LogicalPlan joinWithNext(LogicalPlan source,
                                            QueriedRelation nextRel,
                                            Set<Symbol> usedColumns,
                                            Set<QualifiedName> joinNames,
                                            Map<Set<QualifiedName>, JoinPair> joinPairs,
                                            Map<Set<QualifiedName>, Symbol> queryParts) {
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
            type = joinPair.joinType();
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

        LogicalPlan nextPlan = LogicalPlanner.plan(nextRel, FetchMode.NEVER, false).build(usedFromNext);
        return maybeFilter(
            new Join(source, nextPlan, type, condition),
            removeMatch(queryParts, joinNames, nextName),
            queryParts.remove(Collections.singleton(nextName))
        );
    }

    private static LogicalPlan maybeFilter(LogicalPlan source, @Nullable Symbol filter1, @Nullable Symbol filter2) {
        return Filter.create(
            source,
            AndOperator.join(
                Stream.of(filter1, filter2).filter(Objects::nonNull).iterator()));
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

    private Join(LogicalPlan lhs, LogicalPlan rhs, JoinType joinType, @Nullable Symbol joinCondition) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        if (joinType == JoinType.SEMI) {
            this.outputs = lhs.outputs();
        } else {
            this.outputs = Lists2.concat(lhs.outputs(), rhs.outputs());
        }
        this.baseTables = new ArrayList<>();
        this.baseTables.addAll(lhs.baseTables());
        this.baseTables.addAll(rhs.baseTables());
        this.expressionMapping = new HashMap<>();
        this.expressionMapping.putAll(lhs.expressionMapping());
        this.expressionMapping.putAll(rhs.expressionMapping());
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {
        Plan left = lhs.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null, null);
        Plan right = rhs.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null, null);

        // TODO: distribution planning
        List<String> nlExecutionNodes = Collections.singletonList(plannerContext.handlerNode());
        Symbol joinInput = null;
        if (joinCondition != null) {
            joinInput = InputColumns.create(joinCondition, Lists2.concat(lhs.outputs(), rhs.outputs()));
        }
        NestedLoopPhase nlPhase = new NestedLoopPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "nestedLoop",
            // NestedLoopPhase ctor want's at least one projection
            Collections.singletonList(new EvalProjection(InputColumn.fromSymbols(outputs))),
            receiveResultFrom(plannerContext, left.resultDescription(), nlExecutionNodes),
            receiveResultFrom(plannerContext, right.resultDescription(), nlExecutionNodes),
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

    private static MergePhase receiveResultFrom(Planner.Context plannerContext,
                                                ResultDescription resultDescription,
                                                Collection<String> executionNodes) {
        final List<Projection> projections;
        if (resultDescription.hasRemainingLimitOrOffset()) {
            projections = Collections.singletonList(ProjectionBuilder.topNOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        } else {
            projections = Collections.emptyList();
            if (resultDescription.nodeIds().equals(executionNodes)) {
                return null;
            }
        }
        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "nl-receive-source-result",
            resultDescription.nodeIds().size(),
            executionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan lhsCollapsed = lhs.tryCollapse();
        LogicalPlan rhsCollapsed = rhs.tryCollapse();
        if (lhs != lhsCollapsed || rhs != rhsCollapsed) {
            return new Join(lhsCollapsed, rhsCollapsed, joinType, joinCondition);
        }
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return expressionMapping;
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return baseTables;
    }
}
