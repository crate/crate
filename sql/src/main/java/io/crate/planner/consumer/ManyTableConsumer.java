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

import com.google.common.collect.*;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.sql.tree.QualifiedName;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class ManyTableConsumer implements Consumer {

    private static final Logger LOGGER = Loggers.getLogger(ManyTableConsumer.class);

    private final Visitor visitor;

    ManyTableConsumer() {
        this.visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    /**
     * returns a new collection with the same items as relations contains but in an order which
     * allows the most join condition push downs (assuming that a left-based tree is built later on)
     *
     * @param relations               all relations, e.g. [t1, t2, t3, t3]
     * @param implicitJoinedRelations contains all relations that have a join condition e.g. {{t1, t2}, {t2, t3}}
     * @param joinPairs               contains a list of {@link JoinPair}.
     * @param preSorted               a ordered subset of the relations. The result will start with those relations.
     *                                E.g. [t3] - This would cause the result to start with [t3]
     */
    static Collection<QualifiedName> orderByJoinConditions(Collection<QualifiedName> relations,
                                                           Set<? extends Set<QualifiedName>> implicitJoinedRelations,
                                                           List<JoinPair> joinPairs,
                                                           Collection<QualifiedName> preSorted) {
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
     * select t1.a, t2.b, t3.c from t1, t2, t3
     * </code>
     * <p>
     * a TwoTableJoin tree is built:
     * <p>
     * </code>
     * join(
     * join(t1, t2),
     * t3
     * )
     * </code>
     * <p>
     * Where:
     * <code>
     * join(t1, t2)
     * has:
     * QS: [ RC(t1, 0), RC(t2, 0) ]
     * t1: select a from t1
     * t2: select b from t2
     * </code>
     * <p>
     * and
     * <code>
     * join(join(t1, t2), t3)
     * has:
     * QS: [ RC(join(t1, t2), 0), RC(join(t1, t2), 1),  RC(t3, 0) ]
     * join(t1, t2) -
     * t3: select c from t3
     * <p>
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
        QueriedRelation leftRelation = (QueriedRelation) mss.sources().get(leftName);
        QuerySpec leftQuerySpec = leftRelation.querySpec();
        Optional<RemainingOrderBy> remainingOrderBy = mss.remainingOrderBy();
        List<JoinPair> joinPairs = mss.joinPairs();
        List<TwoTableJoin> twoTableJoinList = new ArrayList<>(orderedRelationNames.size());

        QualifiedName rightName;
        QueriedRelation rightRelation;
        while (it.hasNext()) {
            rightName = it.next();
            rightRelation = (QueriedRelation) mss.sources().get(rightName);

            // process where clause
            Set<QualifiedName> names = Sets.newHashSet(leftName, rightName);
            Predicate<Symbol> predicate = new SubSetOfQualifiedNamesPredicate(names);
            QuerySpec newQuerySpec = rootQuerySpec.subset(predicate, it.hasNext());
            if (splitQuery.containsKey(names)) {
                Symbol symbol = splitQuery.remove(names);
                newQuerySpec.where(new WhereClause(symbol));
            }

            Optional<OrderBy> remainingOrderByToApply = Optional.empty();
            if (remainingOrderBy.isPresent() && remainingOrderBy.get().validForRelations(names)) {
                remainingOrderByToApply = Optional.of(remainingOrderBy.get().orderBy());
                remainingOrderBy = Optional.empty();
            }

            // get explicit join definition
            JoinPair joinPair = JoinPairs.ofRelationsWithMergedConditions(leftName, rightName, joinPairs, true);

            JoinPairs.removeOrderByOnOuterRelation(leftName, rightName, leftQuerySpec, rightRelation.querySpec(), joinPair);

            // NestedLoop will add NULL rows - so order by needs to be applied after the NestedLoop
            TwoTableJoin join = new TwoTableJoin(
                newQuerySpec,
                leftRelation,
                rightRelation,
                remainingOrderByToApply,
                joinPair
            );

            assert leftQuerySpec != null : "leftQuerySpec must not be null";

            /*
             * Create a new QuerySpec & update fields to point to the newly created TwoTableJoin relation.
             *
             * The names of the field are prefixed with their "source" relationName so that they're still unique.
             *
             * Example:
             *
             *     select t1.x, t2.x, t3.x
             *
             *     ->
             *
             *     twoTableJoin.outputs: [ [join.t1.t2].t1.x,  [join.t1.t2].t2.x, t3.x ]
             */
            if (it.hasNext()) { // The outer left join becomes the root {@link TwoTableJoin}
                final AnalyzedRelation left = leftRelation;
                final AnalyzedRelation right = rightRelation;

                Function<? super Symbol, ? extends Symbol> replaceFunction = FieldReplacer.bind(f -> {
                    if (f.relation().equals(left) || f.relation().equals(right)) {
                        // path is prefixed with relationName so that they are still unique
                        ColumnIdent path = new ColumnIdent(f.relation().getQualifiedName().toString(), f.path().outputName());
                        Field field = join.getField(path, Operation.READ);
                        assert field != null : "must be able to resolve the field from the twoTableJoin";
                        return field;
                    }
                    return f;
                });

                splitQuery =
                    rewriteSplitQueryNames(splitQuery, leftName, rightName, join.getQualifiedName(), replaceFunction);
                JoinPairs.rewriteNames(leftName, rightName, join.getQualifiedName(), replaceFunction, joinPairs);
                rewriteOrderByNames(remainingOrderBy, leftName, rightName, join.getQualifiedName(), replaceFunction);
                rootQuerySpec = rootQuerySpec.copyAndReplace(replaceFunction);
                leftQuerySpec = newQuerySpec.copyAndReplace(replaceFunction);
            }
            leftRelation = join;
            leftName = join.getQualifiedName();
            twoTableJoinList.add(join);
        }
        TwoTableJoin join = (TwoTableJoin) leftRelation;
        if (!splitQuery.isEmpty()) {
            join.querySpec().where(new WhereClause(AndOperator.join(splitQuery.values())));
        }

        // Find the last join pair that contains a filtering
        int index = 0;
        for (int i = twoTableJoinList.size() - 1; i >=0; i--) {
            index = i;
            WhereClause where = twoTableJoinList.get(i).querySpec().where();
            if (where.hasQuery() && !(where.query() instanceof Literal)) {
                break;
            }
        }
        // Remove limit from all join pairs before the last filtered one
        for (int i = 0; i < index; i++) {
            twoTableJoinList.get(i).querySpec().limit(Optional.empty());
        }

        return join;
    }

    private static Map<Set<QualifiedName>, Symbol> rewriteSplitQueryNames(Map<Set<QualifiedName>, Symbol> splitQuery,
                                                                          QualifiedName leftName,
                                                                          QualifiedName rightName,
                                                                          QualifiedName newName,
                                                                          java.util.function.Function<? super Symbol, ? extends Symbol> replaceFunction) {
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
                                            Function<? super Symbol, ? extends Symbol> replaceFunction) {
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
        assert mss.sources().size() == 2 : "number of mss.sources() must be 2";
        Iterator<QualifiedName> it = getOrderedRelationNames(mss, ImmutableSet.of()).iterator();
        QualifiedName left = it.next();
        QualifiedName right = it.next();
        JoinPair joinPair = JoinPairs.ofRelationsWithMergedConditions(left, right, mss.joinPairs(), true);
        QueriedRelation leftRelation = (QueriedRelation) mss.sources().get(left);
        QueriedRelation rightRelation = (QueriedRelation) mss.sources().get(right);

        JoinPairs.removeOrderByOnOuterRelation(left, right, leftRelation.querySpec(), rightRelation.querySpec(), joinPair);

        Optional<OrderBy> remainingOrderByToApply = Optional.empty();
        if (mss.remainingOrderBy().isPresent() &&
            mss.remainingOrderBy().get().validForRelations(Sets.newHashSet(left, right))) {
            remainingOrderByToApply = Optional.of(mss.remainingOrderBy().get().orderBy());
        }

        return new TwoTableJoin(
            mss.querySpec(),
            leftRelation,
            rightRelation,
            remainingOrderByToApply,
            joinPair
        );
    }

    private static class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, ConsumerContext context) {
            if (isUnsupportedStatement(mss, context)) return null;

            if (mss.canBeFetched().isEmpty() || context.fetchMode() == FetchMode.NEVER) {
                return getPlan(mss, context);
            }

            context.setFetchMode(FetchMode.NEVER);
            FetchPushDown.Builder<MultiSourceSelect> builder = FetchPushDown.pushDown(mss);
            if (builder == null) {
                return getPlan(mss, context);
            }
            Planner.Context plannerContext = context.plannerContext();
            Plan plan = Merge.ensureOnHandler(getPlan(builder.replacedRelation(), context), plannerContext);

            FetchPushDown.PhaseAndProjection phaseAndProjection = builder.build(plannerContext);
            plan.addProjection(
                phaseAndProjection.projection,
                null,
                null,
                null
            );
            return new QueryThenFetch(plan,  phaseAndProjection.phase);
        }

        private static Plan getPlan(MultiSourceSelect mss, ConsumerContext context) {
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
                context.validationException(new ValidationException("AGGREGATIONS on JOINS are not supported"));
                return true;
            }

            return false;
        }

        private static Plan planSubRelation(ConsumerContext context, TwoTableJoin relation) {
            return context.plannerContext().planSubRelation(relation, context);
        }

    }

    private static class SubSetOfQualifiedNamesPredicate implements Predicate<Symbol> {
        private final Set<QualifiedName> qualifiedNames;
        private final HashSet<QualifiedName> foundNames;

        SubSetOfQualifiedNamesPredicate(Set<QualifiedName> qualifiedNames) {
            this.qualifiedNames = qualifiedNames;
            foundNames = new HashSet<>();
        }

        @Override
        public boolean test(@Nullable Symbol input) {
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
        public Void visitField(Field field, Set<QualifiedName> context) {
            context.add(field.relation().getQualifiedName());
            return null;
        }
    }
}
