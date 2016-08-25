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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.RelationColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.*;

public class ManyTableConsumer implements Consumer {

    private static final ESLogger LOGGER = Loggers.getLogger(ManyTableConsumer.class);

    private final Visitor visitor;

    ManyTableConsumer(ConsumingPlanner consumingPlanner) {
        this.visitor = new Visitor(consumingPlanner);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    /**
     * returns a new collection with the same items as relations contains but in an order which
     * allows the most join condition push downs (assuming that a left-based tree is built later on)
     *
     *
     * @param relations all relations, e.g. [t1, t2, t3, t3]
     * @param joinPairs contains a list of {@link JoinPair}.
     * @param preSorted a ordered subset of the relations. The result will start with those relations.
     *                  E.g. [t3] - This would cause the result to start with [t3]
     */
    static Collection<QualifiedName> orderByJoinConditions(Collection<QualifiedName> relations,
                                                           Set<? extends Set<QualifiedName>> implicitJoinedRelations,
                                                           List<JoinPair> joinPairs,
                                                           Collection<QualifiedName> preSorted) {
        if (preSorted.isEmpty()) {
            return relations;
        }
        if (relations.size() == preSorted.size()) {
            return preSorted;
        }
        if (relations.size() == 2 || (joinPairs.isEmpty() && implicitJoinedRelations.isEmpty())) {
            LinkedHashSet<QualifiedName> qualifiedNames = new LinkedHashSet<>(preSorted);
            qualifiedNames.addAll(relations);
            return qualifiedNames;
        }

        // Create a Copy to ensure equals works correctly for the subList check below.
        preSorted = ImmutableList.copyOf(preSorted);
        Set<QualifiedName> pair = new HashSet<>(2);
        Set<QualifiedName> outerJoinRelations = JoinPairs.outerJoinRelations(joinPairs);
        Collection<QualifiedName> bestOrder = null;
        int best = -1;
        outerloop:
        for (List<QualifiedName> permutation : Collections2.permutations(relations)) {
            if (!preSorted.equals(permutation.subList(0, preSorted.size()))) {
                continue;
            }
            int joinPushDowns = 0;
            for (int i = 0; i < permutation.size() - 1; i++) {
                QualifiedName a = permutation.get(i);
                QualifiedName b = permutation.get(i + 1);

                JoinPair joinPair = JoinPairs.ofRelations(a, b, joinPairs, false);
                if (joinPair == null) {
                    // relations are not directly joined, lets check if they are part of an outer join
                    if (outerJoinRelations.contains(a) || outerJoinRelations.contains(b)) {
                        // part of an outer join, don't change pairs, permutation not possible
                        continue outerloop;
                    } else {
                        pair.clear();
                        pair.add(a);
                        pair.add(b);
                        joinPushDowns += implicitJoinedRelations.contains(pair) ? 1 : 0;
                    }
                } else {
                    // relations are directly joined
                    joinPushDowns += 1;
                }
            }
            if (joinPushDowns == relations.size() - 1) {
                return permutation;
            }
            if (joinPushDowns > best) {
                best = joinPushDowns;
                bestOrder = permutation;
            }
        }
        if (bestOrder == null) {
            bestOrder = relations;
        }
        return bestOrder;
    }

    private static Collection<QualifiedName> getNamesFromOrderBy(OrderBy orderBy) {
        Set<QualifiedName> orderByOrder = new LinkedHashSet<>();
        Set<QualifiedName> names = new HashSet<>();
        for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
            names.clear();
            QualifiedNameCounter.INSTANCE.process(orderBySymbol, names);
            orderByOrder.addAll(names);
        }
        return orderByOrder;
    }

    private static Collection<QualifiedName> getOrderedRelationNames(MultiSourceSelect statement,
                                                                     Set<? extends Set<QualifiedName>> relationPairs) {
        Collection<QualifiedName> orderedRelations = ImmutableList.of();
        Optional<OrderBy> orderBy = statement.querySpec().orderBy();
        if (orderBy.isPresent()) {
            orderedRelations = getNamesFromOrderBy(orderBy.get());
        }
        return orderByJoinConditions(statement.sources().keySet(), relationPairs, statement.joinPairs(), orderedRelations);
    }

    /**
     * build a TwoTableJoin tree.
     * E.g. given a MSS with 3 tables:
     * <code>
     *     select t1.a, t2.b, t3.c from t1, t2, t3
     * </code>
     *
     * a TwoTableJoin tree is built:
     *
     * </code>
     *      join(
     *          join(t1, t2),
     *          t3
     *      )
     * </code>
     *
     * Where:
     * <code>
     *      join(t1, t2)
     *      has:
     *       QS: [ RC(t1, 0), RC(t2, 0) ]
     *       t1: select a from t1
     *       t2: select b from t2
     * </code>
     *
     * and
     * <code>
     *      join(join(t1, t2), t3)
     * has:
     *      QS: [ RC(join(t1, t2), 0), RC(join(t1, t2), 1),  RC(t3, 0) ]
     *      join(t1, t2) -
     *      t3: select c from t3
     *
     * </code>
     */
    static TwoTableJoin buildTwoTableJoinTree(MultiSourceSelect mss) {
        Map<Set<QualifiedName>, Symbol> splitQuery = ImmutableMap.of();
        if (mss.querySpec().where().hasQuery()) {
            splitQuery = QuerySplitter.split(mss.querySpec().where().query());
            mss.querySpec().where(WhereClause.MATCH_ALL);
        }

        Collection<QualifiedName> orderedRelationNames = getOrderedRelationNames(mss, splitQuery.keySet());
        Iterator<QualifiedName> it = orderedRelationNames.iterator();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("relations={} orderedRelations={}", mss.sources().keySet(), orderedRelationNames);
        }

        QualifiedName leftName = it.next();
        QuerySpec rootQuerySpec = mss.querySpec();
        MultiSourceSelect.Source leftSource = mss.sources().get(leftName);
        AnalyzedRelation leftRelation = leftSource.relation();
        QuerySpec leftQuerySpec = leftSource.querySpec();
        Optional<RemainingOrderBy> remainingOrderBy = mss.remainingOrderBy();
        List<JoinPair> joinPairs = mss.joinPairs();

        QualifiedName rightName;
        MultiSourceSelect.Source rightSource;
        while (it.hasNext()) {
            rightName = it.next();
            rightSource = mss.sources().get(rightName);

            // process where clause
            Set<QualifiedName> names = Sets.newHashSet(leftName, rightName);
            Predicate<Symbol> predicate = new SubSetOfQualifiedNamesPredicate(names);
            QuerySpec newQuerySpec = rootQuerySpec.subset(predicate, it.hasNext());
            if (splitQuery.containsKey(names)) {
                Symbol symbol = splitQuery.remove(names);
                newQuerySpec.where(new WhereClause(symbol));
            }

            Optional<OrderBy> remainingOrderByToApply = Optional.absent();
            if (remainingOrderBy.isPresent() && remainingOrderBy.get().validForRelations(names)) {
                remainingOrderByToApply = Optional.of(remainingOrderBy.get().orderBy());
                remainingOrderBy = Optional.absent();
            }

            // get explicit join definition
            JoinPair joinPair = JoinPairs.ofRelationsWithMergedConditions(leftName, rightName, joinPairs);

            JoinPairs.removeOrderByOnOuterRelation(leftName, rightName, leftQuerySpec, rightSource.querySpec(), joinPair);

            // NestedLoop will add NULL rows - so order by needs to be applied after the NestedLoop
            TwoTableJoin join = new TwoTableJoin(
                newQuerySpec,
                new MultiSourceSelect.Source(leftRelation, leftQuerySpec),
                rightSource,
                remainingOrderByToApply,
                joinPair
            );

            assert leftQuerySpec != null;
            final RelationColumnReWriteCtx reWriteCtx = new RelationColumnReWriteCtx(join);
            Function<? super Symbol, Symbol> replaceFunction = new Function<Symbol, Symbol>() {
                @Nullable
                @Override
                public Symbol apply(@Nullable Symbol input) {
                    if (input == null) {
                        return null;
                    }
                    return RelationColumnReWriter.INSTANCE.process(input, reWriteCtx);
                }
            };

            /**
             * Rewrite where, join and order by clauses and create a new query spec, where all RelationColumn symbols
             * with a QualifiedName of {@link RelationColumnReWriteCtx#left} or {@link RelationColumnReWriteCtx#right}
             * are replaced with a RelationColumn with QualifiedName of {@link RelationColumnReWriteCtx#newName}
             */
            splitQuery = rewriteSplitQueryNames(splitQuery, leftName, rightName, join.name(), replaceFunction);
            JoinPairs.rewriteNames(leftName, rightName, join.name(), replaceFunction, joinPairs);
            rewriteOrderByNames(remainingOrderBy, leftName, rightName, join.name(), replaceFunction);
            rootQuerySpec = rootQuerySpec.copyAndReplace(replaceFunction);
            leftQuerySpec = newQuerySpec.copyAndReplace(replaceFunction);
            leftRelation = join;
            leftName = join.name();
        }
        TwoTableJoin join = (TwoTableJoin) leftRelation;
        if (!splitQuery.isEmpty()) {
            join.querySpec().where(new WhereClause(AndOperator.join(splitQuery.values())));
        }
        return join;
    }

    private static Map<Set<QualifiedName>, Symbol> rewriteSplitQueryNames(Map<Set<QualifiedName>, Symbol> splitQuery,
                                                                    QualifiedName leftName,
                                                                    QualifiedName rightName,
                                                                    QualifiedName newName,
                                                                    Function<? super Symbol, Symbol> replaceFunction) {
        Map<Set<QualifiedName>, Symbol> newMap = new HashMap<>(splitQuery.size());
        for (Map.Entry<Set<QualifiedName>, Symbol> entry : splitQuery.entrySet()) {
            Set<QualifiedName> key = entry.getKey();
            replace(leftName, newName, key);
            replace(rightName, newName, key);
            if (newMap.containsKey(key)) {
                newMap.put(key, AndOperator.join(Arrays.asList(newMap.get(key), replaceFunction.apply(entry.getValue()))));
            } else {
                newMap.put(key, replaceFunction.apply(entry.getValue()));
            }
        }
        return newMap;
    }

    private static void rewriteOrderByNames(Optional<RemainingOrderBy> remainingOrderBy,
                                            QualifiedName leftName,
                                            QualifiedName rightName,
                                            QualifiedName newName,
                                            Function<? super Symbol, Symbol> replaceFunction) {
        if (remainingOrderBy.isPresent()) {
            Set<QualifiedName> relations = remainingOrderBy.get().relations();
            replace(leftName, newName, relations);
            replace(rightName, newName, relations);
            remainingOrderBy.get().orderBy().replace(replaceFunction);
        }
    }

    private static void replace(QualifiedName oldName, QualifiedName newName, Set<QualifiedName> s) {
        if (s.contains(oldName)) {
            s.remove(oldName);
            s.add(newName);
        }
    }

    static TwoTableJoin twoTableJoin(MultiSourceSelect mss) {
        assert mss.sources().size() == 2;
        Iterator<QualifiedName> it = getOrderedRelationNames(mss, ImmutableSet.<Set<QualifiedName>>of()).iterator();
        QualifiedName left = it.next();
        QualifiedName right = it.next();
        JoinPair joinPair = JoinPairs.ofRelationsWithMergedConditions(left, right, mss.joinPairs());
        MultiSourceSelect.Source leftSource = mss.sources().get(left);
        MultiSourceSelect.Source rightSource = mss.sources().get(right);

        JoinPairs.removeOrderByOnOuterRelation(left, right, leftSource.querySpec(), rightSource.querySpec(), joinPair);

        // TODO: enable optimization for all join types after RIGHT OUTER JOIN is implemented
        if (joinPair.joinType().ordinal() < JoinType.INNER.ordinal()) {
            it = getOrderedRelationNames(mss, ImmutableSet.<Set<QualifiedName>>of()).iterator();
            left = it.next();
            right = it.next();
        }

        Optional<OrderBy> remainingOrderByToApply = Optional.absent();
        if (mss.remainingOrderBy().isPresent() && mss.remainingOrderBy().get().validForRelations(Sets.newHashSet(left, right))) {
            remainingOrderByToApply = Optional.of(mss.remainingOrderBy().get().orderBy());
        }

        return new TwoTableJoin(
            mss.querySpec(),
            leftSource,
            rightSource,
            remainingOrderByToApply,
            joinPair
        );
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ConsumingPlanner consumingPlanner;

        public Visitor(ConsumingPlanner consumingPlanner) {
            this.consumingPlanner = consumingPlanner;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect mss, ConsumerContext context) {
            if (isUnsupportedStatement(mss, context)) return null;
            replaceFieldsWithRelationColumns(mss);
            if (mss.sources().size() == 2) {
                return planSubRelation(context, twoTableJoin(mss));
            }
            return planSubRelation(context, buildTwoTableJoinTree(mss));
        }


        private static boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.querySpec().groupBy().isPresent()) {
                context.validationException(new ValidationException("GROUP BY on JOINS is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on JOINS is not supported"));
                return true;
            }

            return false;
        }

        private PlannedAnalyzedRelation planSubRelation(ConsumerContext context, AnalyzedRelation relation) {
            if (context.isRoot()) {
                return consumingPlanner.plan(relation, context);
            }
            return context.plannerContext().planSubRelation(relation, context);
        }

    }

    static void replaceFieldsWithRelationColumns(MultiSourceSelect mss) {
        final FieldToRelationColumnCtx ctx = new FieldToRelationColumnCtx(mss);
        Function<Symbol, Symbol> replaceFunction = new Function<Symbol, Symbol>() {
            @Nullable
            @Override
            public Symbol apply(@Nullable Symbol input) {
                if (input == null) {
                    return null;
                }
                return FieldToRelationColumnVisitor.INSTANCE.process(input, ctx);
            }
        };

        if (mss.remainingOrderBy().isPresent()) {
            mss.remainingOrderBy().get().orderBy().replace(replaceFunction);
        }
        for (JoinPair joinPair : mss.joinPairs()) {
            joinPair.replaceCondition(replaceFunction);
        }
        mss.querySpec().replace(replaceFunction);
    }

    private static class SubSetOfQualifiedNamesPredicate implements Predicate<Symbol> {
        private final Set<QualifiedName> qualifiedNames;
        private final HashSet<QualifiedName> foundNames;

        SubSetOfQualifiedNamesPredicate(Set<QualifiedName> qualifiedNames) {
            this.qualifiedNames = qualifiedNames;
            foundNames = new HashSet<>();
        }

        @Override
        public boolean apply(@Nullable Symbol input) {
            if (input == null) {
                return false;
            }
            foundNames.clear();
            QualifiedNameCounter.INSTANCE.process(input, foundNames);
            return Sets.difference(foundNames, qualifiedNames).isEmpty();
        }
    }

    public static class QualifiedNameCounter extends DefaultTraversalSymbolVisitor<Set<QualifiedName>, Void> {
        public static final QualifiedNameCounter INSTANCE = new QualifiedNameCounter();

        @Override
        public Void visitRelationColumn(RelationColumn relationColumn, Set<QualifiedName> context) {
            context.add(relationColumn.relationName());
            return null;
        }
    }

    private static class RelationColumnReWriteCtx {
        private final QualifiedName newName;
        private final QualifiedName left;
        private final QualifiedName right;
        private final int rightOffset;

        RelationColumnReWriteCtx(TwoTableJoin join) {
            this(join.name(), join.leftName(), join.rightName(), join.left().querySpec().outputs().size());
        }

        RelationColumnReWriteCtx(QualifiedName newName, QualifiedName left, QualifiedName right, int rightOffset) {
            this.newName = newName;
            this.left = left;
            this.right = right;
            this.rightOffset = rightOffset;
        }
    }

    private static class RelationColumnReWriter extends ReplacingSymbolVisitor<RelationColumnReWriteCtx> {

        private static final RelationColumnReWriter INSTANCE = new RelationColumnReWriter(false);

        RelationColumnReWriter(boolean inPlace) {
            super(inPlace);
        }

        @Override
        public Symbol visitRelationColumn(RelationColumn relationColumn, RelationColumnReWriteCtx context) {
            if (relationColumn.relationName().equals(context.left)) {
                return new RelationColumn(context.newName, relationColumn.index(), relationColumn.valueType());
            }
            if (relationColumn.relationName().equals(context.right)) {
                return new RelationColumn(context.newName, relationColumn.index() + context.rightOffset, relationColumn.valueType());
            }
            return super.visitRelationColumn(relationColumn, context);
        }
    }

    private static class FieldToRelationColumnCtx {
        private final Map<AnalyzedRelation, QualifiedName> relationToName;
        private final MultiSourceSelect mss;

        FieldToRelationColumnCtx(MultiSourceSelect mss) {
            relationToName = new IdentityHashMap<>(mss.sources().size());
            for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : mss.sources().entrySet()) {
                relationToName.put(entry.getValue().relation(), entry.getKey());
            }
            this.mss = mss;
        }
    }

    private static class FieldToRelationColumnVisitor extends ReplacingSymbolVisitor<FieldToRelationColumnCtx> {

        private static final FieldToRelationColumnVisitor INSTANCE = new FieldToRelationColumnVisitor(false);

        FieldToRelationColumnVisitor(boolean inPlace) {
            super(inPlace);
        }

        @Override
        public Symbol visitField(Field field, FieldToRelationColumnCtx ctx) {
            QualifiedName qualifiedName = ctx.relationToName.get(field.relation());
            int idx = 0;
            for (Symbol symbol : ctx.mss.sources().get(qualifiedName).querySpec().outputs()) {
                if (symbol instanceof Field) {
                    if (((Field) symbol).path().equals(field.path())) {
                        return new RelationColumn(qualifiedName, idx, field.valueType());
                    }
                }
                idx++;
            }
            return field;
        }
    }
}
