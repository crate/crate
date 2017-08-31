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

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.TwoTableJoin;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.JoinPairs;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.analyze.relations.RemainingOrderBy;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.MatchPredicate;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.UnsupportedFeatureException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
     * Returns a new collection with the same items as relations contains but in the best possible order.
     * <p>
     * Assuming that a left-based tree is built later on:
     *  IF there is no `ORDER BY`:
     *      IF no join conditions:
     *          Don't change the order.
     *      ELSE:
     *          Return the relation in the order specified by the join conditions between them.
     *  ELSE:
     *      # If an `ORDER BY` exists then the {@param preSorted} list contains the relations that are referenced in the
     *      # ORDER BY in the order they are used in its symbols.
     *
     *      IF all relations contained {@param preSorted}:
     *          Return the {@param preSorted} ordering
     *      ELSE:
     *          Keep the "prefix" {@param preSorted} ordering and then find the best order possible based on the most
     *          join conditions pushed down in the final left-based join tree.
     *
     * @param relations               all relations, e.g. [t1, t2, t3, t3]
     * @param explicitJoinedRelations contains all relation pairs that have an explicit join condition
     *                                e.g. {{t1, t2}, {t2, t3}}
     * @param implicitJoinedRelations contains all relations pairs that have an implicit join condition
     *                                e.g. {{t1, t2}, {t2, t3}}
     * @param preSorted               a ordered subset of the relations. The result will start with those relations.
     *                                E.g. [t3] - This would cause the result to start with [t3]
     */
    static Collection<QualifiedName> orderByJoinConditions(Collection<QualifiedName> relations,
                                                           Set<? extends Set<QualifiedName>> explicitJoinedRelations,
                                                           Set<? extends Set<QualifiedName>> implicitJoinedRelations,
                                                           Collection<QualifiedName> preSorted) {
        // All relations already sorted based the `ORDER BY` symbols
        if (relations.size() == preSorted.size()) {
            return preSorted;
        }

        // Only 2 relations or the relations have no join conditions (explicit or implicit) between them
        if (relations.size() == 2 ||
            (explicitJoinedRelations.isEmpty() && implicitJoinedRelations.isEmpty())) {
            LinkedHashSet<QualifiedName> qualifiedNames = new LinkedHashSet<>(preSorted);
            qualifiedNames.addAll(relations);
            return qualifiedNames;
        }

        LinkedHashSet<QualifiedName> bestOrder = new LinkedHashSet<>();
        List<Set<QualifiedName>> pairsWithJoinConditions = new ArrayList<>(explicitJoinedRelations);
        pairsWithJoinConditions.addAll(implicitJoinedRelations);

        // If no `ORDER BY` present we have no preSort to follow so we return the relations in ordering
        // obtained by the join conditions (explicit and/or implicit) between them.
        if (preSorted.isEmpty()) {
            ObjectIntHashMap<QualifiedName> occurrences =
                getOccurrencesInJoinConditions(relations.size(), explicitJoinedRelations, implicitJoinedRelations);

            Set<QualifiedName> firstJoinPair = findAndRemoveFirstJoinPair(occurrences, pairsWithJoinConditions);
            assert firstJoinPair != null : "firstJoinPair should not be null";
            bestOrder.addAll(firstJoinPair);
        } else {
            bestOrder.addAll(preSorted);
        }

        buildBestOrderByJoinConditions(pairsWithJoinConditions, bestOrder);
        bestOrder.addAll(relations);
        return bestOrder;
    }

    private static ObjectIntHashMap<QualifiedName> getOccurrencesInJoinConditions(
        int numberOfRelations,
        Set<? extends Set<QualifiedName>> explicitJoinedRelations,
        Set<? extends Set<QualifiedName>> implicitJoinedRelations) {

        ObjectIntHashMap<QualifiedName> occurrences = new ObjectIntHashMap<>(numberOfRelations);
        explicitJoinedRelations.forEach(o -> o.forEach(qName -> occurrences.putOrAdd(qName, 1, 1)));
        implicitJoinedRelations.forEach(o -> o.forEach(qName -> occurrences.putOrAdd(qName, 1, 1)));
        return occurrences;
    }

    @VisibleForTesting
    static Set<QualifiedName> findAndRemoveFirstJoinPair(ObjectIntHashMap<QualifiedName> occurrences,
                                                         Collection<Set<QualifiedName>> joinPairs) {
        Iterator<Set<QualifiedName>> setsIterator = joinPairs.iterator();
        while (setsIterator.hasNext()) {
            Set<QualifiedName> set = setsIterator.next();
            for (QualifiedName name : set) {
                int count = occurrences.getOrDefault(name, 0);
                if (count > 1) {
                    setsIterator.remove();
                    return set;
                }
            }
        }
        return joinPairs.iterator().next();
    }

    private static void buildBestOrderByJoinConditions(List<Set<QualifiedName>> sets, LinkedHashSet<QualifiedName> bestOrder) {
        Iterator<Set<QualifiedName>> setsIterator = sets.iterator();
        while (setsIterator.hasNext()) {
            Set<QualifiedName> set = setsIterator.next();
            for (QualifiedName name: set) {
                if (bestOrder.contains(name)) {
                    bestOrder.addAll(set);
                    setsIterator.remove();
                    setsIterator = sets.iterator();
                    break;
                }
            }
        }

        // Add the rest of the relations to the end of the collection
        sets.forEach(bestOrder::addAll);
    }

    @VisibleForTesting
    static Collection<QualifiedName> getNamesFromOrderBy(OrderBy orderBy, List<JoinPair> joinPairs) {
        Set<QualifiedName> outerJoinRelations = JoinPairs.outerJoinRelations(joinPairs);
        Set<QualifiedName> orderByOrder = new LinkedHashSet<>();
        Set<QualifiedName> names = new LinkedHashSet<>();
        for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
            names.clear();
            QualifiedNameCounter.INSTANCE.process(orderBySymbol, names);
            if (validateAndAddToOrderedRelations(orderByOrder, names, joinPairs, outerJoinRelations) == false) {
                return orderByOrder;
            }
        }
        return orderByOrder;
    }

    /**
     * Try to add the {@param relationsToAddToCurrentOrder} to the current ordered collection {@param currentOrdered}.
     * If all relations can be added return true else false.
     *
     * Start adding the relations of {@param relationsToAddToCurrentOrder} to the {@param currentOrdered}.
     * When a violation of the outer join pairs is detected stop adding and possibly remove the last
     * relation of the {@param currentOrdered} list that caused the violation.
     */
    private static boolean validateAndAddToOrderedRelations(Collection<QualifiedName> currentOrdered,
                                                            Collection<QualifiedName> relationsToAddToCurrentOrder,
                                                            List<JoinPair> joinPairs,
                                                            Set<QualifiedName> outerJoinRelations) {
        Iterator<QualifiedName> currentOrderedIterator = relationsToAddToCurrentOrder.iterator();
        QualifiedName a = Iterators.getLast(currentOrderedIterator, null);

        Iterator<QualifiedName> namesIterator = relationsToAddToCurrentOrder.iterator();
        while (namesIterator.hasNext()) {
            if (a == null) {
                a = namesIterator.next();
            }
            if (namesIterator.hasNext()) {
                QualifiedName b = namesIterator.next();
                if (violatesOuterJoins(a, b, joinPairs, outerJoinRelations)) {
                    if (!currentOrdered.isEmpty()) {
                        // Remove also the last element of the existing sorted list
                        currentOrderedIterator.remove();
                    }
                    return false;
                }
                currentOrdered.add(b);
                a = b;
            }
        }
        return true;
    }

    private static boolean violatesOuterJoins(QualifiedName a,
                                              QualifiedName b,
                                              List<JoinPair> joinPairs,
                                              Set<QualifiedName> outerJoinRelations) {
        JoinPair joinPair = JoinPairs.exactFindPair(joinPairs, a, b);
        // relations are not directly joined and they are part of an outer join
        return (joinPair == null && (outerJoinRelations.contains(a) || outerJoinRelations.contains(b)));
    }

    @VisibleForTesting
    static Collection<QualifiedName> getOrderedRelationNames(
            MultiSourceSelect statement,
            Set<? extends Set<QualifiedName>> explicitJoinConditions,
            Set<? extends Set<QualifiedName>> implicitJoinConditions) {

        Collection<QualifiedName> orderedRelations = ImmutableList.of();
        Optional<OrderBy> orderBy = statement.querySpec().orderBy();
        if (orderBy.isPresent()) {
            orderedRelations = getNamesFromOrderBy(orderBy.get(), statement.joinPairs());
        }

        return orderByJoinConditions(
            statement.sources().keySet(),
            explicitJoinConditions,
            implicitJoinConditions,
            orderedRelations);
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
        Map<Set<QualifiedName>, Symbol> splittedWhereQuery = ImmutableMap.of();
        if (mss.querySpec().where().hasQuery()) {
            splittedWhereQuery = QuerySplitter.split(mss.querySpec().where().query());
            mss.querySpec().where(WhereClause.MATCH_ALL);
        }

        List<JoinPair> joinPairs = mss.joinPairs();
        Map<Set<QualifiedName>, Symbol> splittedJoinConditions = buildJoinConditionsMap(joinPairs);
        Collection<QualifiedName> orderedRelationNames =
            getOrderedRelationNames(mss, splittedJoinConditions.keySet(), splittedWhereQuery.keySet());
        Iterator<QualifiedName> it = orderedRelationNames.iterator();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("relations={} orderedRelations={}", mss.sources().keySet(), orderedRelationNames);
        }

        QualifiedName leftName = it.next();
        QuerySpec rootQuerySpec = mss.querySpec();
        QueriedRelation leftRelation = (QueriedRelation) mss.sources().get(leftName);
        QuerySpec leftQuerySpec = leftRelation.querySpec();
        Optional<RemainingOrderBy> remainingOrderBy = mss.remainingOrderBy();
        List<TwoTableJoin> twoTableJoinList = new ArrayList<>(orderedRelationNames.size());
        Set<QualifiedName> currentTreeRelationNames = new HashSet<>(orderedRelationNames.size());
        currentTreeRelationNames.add(leftName);
        QualifiedName rightName;
        QueriedRelation rightRelation;
        while (it.hasNext()) {
            rightName = it.next();
            rightRelation = (QueriedRelation) mss.sources().get(rightName);
            currentTreeRelationNames.add(rightName);

            // process where clause
            Set<QualifiedName> names = Sets.newHashSet(leftName, rightName);
            Predicate<Symbol> predicate = new SubSetOfQualifiedNamesPredicate(names);
            QuerySpec newQuerySpec = rootQuerySpec.subset(predicate, it.hasNext());
            if (splittedWhereQuery.containsKey(names)) {
                Symbol symbol = splittedWhereQuery.remove(names);
                newQuerySpec.where(new WhereClause(symbol));
            }

            if (it.hasNext()) {
                extendQSOutputs(splittedWhereQuery, leftName, rightName, newQuerySpec);
                extendQSOutputs(splittedJoinConditions, leftName, rightName, newQuerySpec);
            }

            Optional<OrderBy> remainingOrderByToApply = Optional.empty();
            if (remainingOrderBy.isPresent() && remainingOrderBy.get().validForRelations(names)) {
                remainingOrderByToApply = Optional.of(remainingOrderBy.get().orderBy());
                remainingOrderBy = Optional.empty();
            }

            // get explicit join definition
            JoinPair joinPair = JoinPairs.findAndRemovePair(joinPairs, leftName, rightName);

            // Search the splittedJoinConditions to find if a join condition
            // can be applied at the current status of the join tree
            List<Symbol> joinConditions = new ArrayList<>();
            for (Iterator<Map.Entry<Set<QualifiedName>, Symbol>> joinConditionEntryIterator =
                 splittedJoinConditions.entrySet().iterator(); joinConditionEntryIterator.hasNext();) {

                Map.Entry<Set<QualifiedName>, Symbol> entry = joinConditionEntryIterator.next();
                if (currentTreeRelationNames.containsAll(entry.getKey())) {
                    joinConditions.add(entry.getValue());
                    joinConditionEntryIterator.remove();
                }
            }
            joinPair.condition(joinConditions.isEmpty() ? null : AndOperator.join(joinConditions));

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

                splittedWhereQuery =
                    rewriteSplitQueryNames(splittedWhereQuery, leftName, rightName, join.getQualifiedName(), replaceFunction);
                JoinPairs.rewriteNames(leftName, rightName, join.getQualifiedName(), replaceFunction, joinPairs);
                rewriteOrderByNames(remainingOrderBy, leftName, rightName, join.getQualifiedName(), replaceFunction);
                rootQuerySpec = rootQuerySpec.copyAndReplace(replaceFunction);
                rewriteJoinConditionNames(splittedJoinConditions, replaceFunction);
            }
            leftRelation = join;
            leftName = join.getQualifiedName();
            twoTableJoinList.add(join);
        }
        TwoTableJoin join = (TwoTableJoin) leftRelation;
        if (!splittedWhereQuery.isEmpty()) {
            join.querySpec().where(new WhereClause(AndOperator.join(splittedWhereQuery.values())));
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


    /**
     * Extends the outputs of a querySpec to include symbols which are required by the next/upper
     * joins in the tree. These are symbols that are not selected, but are for example used in a
     * joinCondition later on.
     *
     * e.g.:
     * select count(*) from t1, t2, t3 where t1.a = t2.b and t2.b = t3.c
     *
     *                   join
     * outputs=t1[a]    /   \
     *      |          /     \
     *      +------> join    t3
     *               / \
     *              /   \
     *             t1   t2
     */
    private static void extendQSOutputs(Map<Set<QualifiedName>, Symbol> splitQuery,
                                        final QualifiedName leftName,
                                        final QualifiedName rightName,
                                        QuerySpec newQuerySpec) {
        Set<Symbol> fields = new LinkedHashSet<>(newQuerySpec.outputs());
        for (Map.Entry<Set<QualifiedName>, Symbol> entry : splitQuery.entrySet()) {
            Set<QualifiedName> relations = entry.getKey();
            Symbol joinCondition = entry.getValue();
            if (relations.contains(leftName) || relations.contains(rightName)) {
                FieldsVisitor.visitFields(joinCondition,
                                          f -> {
                                            if (f.relation().getQualifiedName().equals(leftName) ||
                                                    f.relation().getQualifiedName().equals(rightName)) {
                                                fields.add(f);
                                            }
                                          });
            }
        }
        newQuerySpec.outputs(new ArrayList<>(fields));
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

    private static void rewriteJoinConditionNames(Map<Set<QualifiedName>, Symbol> joinConditionsMap,
                                                  Function<? super Symbol, ? extends Symbol> replaceFunction) {
        joinConditionsMap.replaceAll((qualifiedNames, symbol) -> replaceFunction.apply(symbol));
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
        Iterator<QualifiedName> it = getOrderedRelationNames(mss, ImmutableSet.of(), ImmutableSet.of()).iterator();
        QualifiedName left = it.next();
        QualifiedName right = it.next();
        JoinPair joinPair = JoinPairs.findAndRemovePair(mss.joinPairs(), left, right);
        QueriedRelation leftRelation = (QueriedRelation) mss.sources().get(left);
        QueriedRelation rightRelation = (QueriedRelation) mss.sources().get(right);

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

            if (mss.canBeFetched().isEmpty()) {
                context.setFetchMode(FetchMode.NEVER);
            }
            if (context.fetchMode() == FetchMode.NEVER) {
                return getPlan(mss, context);
            }

            FetchPushDown.Builder<MultiSourceSelect> builder = FetchPushDown.pushDown(mss);
            if (builder == null) {
                return getPlan(mss, context);
            }
            context.setFetchMode(FetchMode.NEVER);
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
                context.validationException(new UnsupportedFeatureException("GROUP BY on JOINS is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new UnsupportedFeatureException("AGGREGATIONS on JOINS are not supported"));
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

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, Set<QualifiedName> context) {
            for (Field field : matchPredicate.identBoostMap().keySet()) {
                context.add(field.relation().getQualifiedName());
            }
            return null;
        }
    }

    /*
     * Builds a Map structure out of all the join conditions where every entry
     * represents the join condition (entry.value()) that can be applied on a set of relations (entry.key())
     *
     * The resulting Map is used to apply as many join conditions and as early as possible during
     * the construction of the join tree.
     */
    @VisibleForTesting
    static Map<Set<QualifiedName>, Symbol> buildJoinConditionsMap(List<JoinPair> joinPairs) {
        Map<Set<QualifiedName>, Symbol> conditionsMap = new LinkedHashMap<>();
        for (JoinPair joinPair : joinPairs) {
            Symbol condition = joinPair.condition();
            if (condition != null) {
                Map<Set<QualifiedName>, Symbol> splitted = QuerySplitter.split(joinPair.condition());
                for (Map.Entry<Set<QualifiedName>, Symbol> entry : splitted.entrySet()) {
                    conditionsMap.merge(entry.getKey(), entry.getValue(),
                                        (a, b) -> AndOperator.join(Arrays.asList(a, b)));
                }
            }
        }
        return conditionsMap;
    }
}
