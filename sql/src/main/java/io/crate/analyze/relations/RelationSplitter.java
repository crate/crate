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

package io.crate.analyze.relations;

import com.google.common.collect.*;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.fetch.FetchFieldExtractor;
import io.crate.analyze.symbol.*;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Limits;
import io.crate.sql.tree.QualifiedName;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class RelationSplitter {

    private final QuerySpec querySpec;
    private final Set<Symbol> requiredForQuery = new HashSet<>();
    private final Map<AnalyzedRelation, QuerySpec> specs;
    private final Map<QualifiedName, AnalyzedRelation> relations;
    private final List<JoinPair> joinPairs;
    private final List<Symbol> joinConditions;
    private Set<Field> canBeFetched;
    private RemainingOrderBy remainingOrderBy;

    private static final Supplier<Set<Integer>> INT_SET_SUPPLIER = HashSet::new;

    public RelationSplitter(QuerySpec querySpec,
                            Collection<? extends AnalyzedRelation> relations,
                            List<JoinPair> joinPairs) {
        this.querySpec = querySpec;
        specs = new IdentityHashMap<>(relations.size());
        this.relations = new HashMap<>(relations.size());
        for (AnalyzedRelation relation : relations) {
            specs.put(relation, new QuerySpec());
            this.relations.put(relation.getQualifiedName(), relation);
        }
        this.joinPairs = joinPairs;
        joinConditions = new ArrayList<>(joinPairs.size());
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.condition() != null) {
                JoinConditionValidator.validate(joinPair.condition());
                joinConditions.add(joinPair.condition());
            }
        }
    }

    public Optional<RemainingOrderBy> remainingOrderBy() {
        return Optional.ofNullable(remainingOrderBy);
    }

    public Set<Symbol> requiredForQuery() {
        return requiredForQuery;
    }

    public Set<Field> canBeFetched() {
        return canBeFetched;
    }

    public QuerySpec getSpec(AnalyzedRelation relation) {
        return specs.get(relation);
    }

    public void process() {
        processOrderBy();
        processWhere();
        processOutputs();
    }

    private QuerySpec getSpec(QualifiedName relationName) {
        return specs.get(relations.get(relationName));
    }

    private void processOutputs() {
        SetMultimap<AnalyzedRelation, Symbol> fieldsByRelation = Multimaps.newSetMultimap(
            new IdentityHashMap<AnalyzedRelation, Collection<Symbol>>(specs.size()), LinkedHashSet::new);
        Consumer<Field> addFieldToMap = f -> fieldsByRelation.put(f.relation(), f);

        // declare all symbols from the remaining order by as required for query
        if (remainingOrderBy != null) {
            OrderBy orderBy = remainingOrderBy.orderBy();

            FieldsVisitor.visitFields(orderBy.orderBySymbols(), f -> {
                fieldsByRelation.put(f.relation(), f);
                requiredForQuery.add(f);
            });
        }

        Optional<List<Symbol>> groupBy = querySpec.groupBy();
        if (groupBy.isPresent()) {
            FieldsVisitor.visitFields(groupBy.get(), addFieldToMap);
        }
        Optional<HavingClause> having = querySpec.having();
        if (having.isPresent()) {
            HavingClause havingClause = having.get();
            if (havingClause.hasQuery()) {
                FieldsVisitor.visitFields(havingClause.query(), addFieldToMap);
            }
        }

        if (querySpec.where().hasQuery()) {
            FieldsVisitor.visitFields(querySpec.where().query(), addFieldToMap);
        }

        // collect all fields from all join conditions
        FieldsVisitor.visitFields(joinConditions, addFieldToMap);

        // set the limit and offset where possible
        Optional<Symbol> limit = querySpec.limit();
        if (limit.isPresent()) {
            Optional<Symbol> limitAndOffset = Limits.mergeAdd(limit, querySpec.offset());
            for (AnalyzedRelation rel : Sets.difference(specs.keySet(), fieldsByRelation.keySet())) {
                QuerySpec spec = specs.get(rel);
                spec.limit(limitAndOffset);
            }
        }

        // add all order by symbols to context outputs
        for (Map.Entry<AnalyzedRelation, QuerySpec> entry : specs.entrySet()) {
            QuerySpec querySpec = entry.getValue();
            if (querySpec.orderBy().isPresent()) {
                FieldsVisitor.visitFields(querySpec.orderBy().get().orderBySymbols(), addFieldToMap);
            }
        }

        // everything except the actual outputs is required for query
        requiredForQuery.addAll(fieldsByRelation.values());

        // capture items from the outputs
        canBeFetched = FetchFieldExtractor.process(querySpec.outputs(), fieldsByRelation);

        FieldsVisitor.visitFields(querySpec.outputs(), addFieldToMap);

        // generate the outputs of the subSpecs
        for (Map.Entry<AnalyzedRelation, QuerySpec> entry : specs.entrySet()) {
            Collection<Symbol> fields = fieldsByRelation.get(entry.getKey());
            assert entry.getValue().outputs() == null : "entry.getValue().outputs() must not be null";
            entry.getValue().outputs(new ArrayList<>(fields));
        }
    }

    private void processWhere() {
        if (!querySpec.where().hasQuery()) {
            return;
        }
        Symbol query = querySpec.where().query();
        assert query != null : "query must not be null";
        QuerySplittingVisitor.Context context = QuerySplittingVisitor.INSTANCE.process(querySpec.where().query(), joinPairs);
        JoinConditionValidator.validate(context.query());
        querySpec.where(new WhereClause(context.query()));
        for (Map.Entry<QualifiedName, Collection<Symbol>> entry : context.queries().asMap().entrySet()) {
            getSpec(entry.getKey()).where(new WhereClause(AndOperator.join(entry.getValue())));
        }
    }

    private void processOrderBy() {
        if (!querySpec.orderBy().isPresent()) {
            return;
        }
        OrderBy orderBy = querySpec.orderBy().get();

        Set<AnalyzedRelation> relations = Collections.newSetFromMap(new IdentityHashMap<AnalyzedRelation, Boolean>());
        Consumer<Field> relationsConsumer = f -> relations.add(f.relation());

        // We copy all ORDER BY symbols to remainingOrderBy
        extractRemainingOrderBy(orderBy, relations, relationsConsumer);

        Multimap<AnalyzedRelation, Integer> splits = extractOrderByForPushDown(orderBy, relations, relationsConsumer);

        // Pushed down the orderBy to subquery specs and also set the limitAndOffset if present
        Optional<Symbol> limitAndOffset = Limits.mergeAdd(querySpec.limit(), querySpec.offset());
        for (Map.Entry<AnalyzedRelation, Collection<Integer>> entry : splits.asMap().entrySet()) {
            AnalyzedRelation relation = entry.getKey();
            OrderBy newOrderBy = orderBy.subset(entry.getValue());
            QuerySpec spec = getSpec(relation);
            assert !spec.orderBy().isPresent() : "spec.orderBy() must not be present";
            spec.orderBy(newOrderBy);
            if (limitAndOffset.isPresent()) {
                spec.limit(limitAndOffset);
            }
            requiredForQuery.addAll(newOrderBy.orderBySymbols());
        }
    }

    private void extractRemainingOrderBy(OrderBy orderBy, Set<AnalyzedRelation> relations, Consumer<Field> relationsConsumer) {
        Integer idx = 0;
        for (Symbol symbol : orderBy.orderBySymbols()) {
            // If order by is pointing to an aggregation we cannot push it down
            // because ordering needs to happen AFTER the join
            if (Aggregations.containsAggregation(symbol)) {
                continue;
            }
            relations.clear();
            FieldsVisitor.visitFields(symbol, relationsConsumer);

            // instantiate remainingOrderBy really only if needed!
            // because it is used as a marker
            if (remainingOrderBy == null) {
                remainingOrderBy = new RemainingOrderBy();
            }

            OrderBy newOrderBy = orderBy.subset(Collections.singletonList(idx));
            for (AnalyzedRelation rel : relations) {
                remainingOrderBy.addRelation(rel.getQualifiedName());
            }
            remainingOrderBy.addOrderBy(newOrderBy);
            idx++;
        }
        relations.clear();
    }

    /**
     * Extract the ORDER BY symbols which can be pushed down to subqueries.
     * To extract them, the symbols are processed from left to right in their stated order.
     * A symbol can only be pushed down if it contains a single relation.
     * Additionally either the previous symbol (from left to right) contained the same relation,
     * or that relation of the symbol did not occur yet.
     * Once a symbols contains more than a single relation no further order by symbols may be pushed down.
     *
     * Examples:
     * ORDER BY t1.a, t1.b, t2.x, t2.y -> t1: a, b, t2: x, y
     * ORDER BY t1.a, t2.x, t1.b, t2.x -> t1: a, t2: x
     * ORDER BY t1.a, fn(t1.a, t2.x), t2.x -> t1: a
     * ORDER BY fn(t1.a, t2.x), t3.z -> none
     */
    private Multimap<AnalyzedRelation, Integer> extractOrderByForPushDown(OrderBy orderBy, Set<AnalyzedRelation> relations, Consumer<Field> relationsConsumer) {
        Multimap<AnalyzedRelation, Integer> splits = Multimaps.newSetMultimap(
            new IdentityHashMap<AnalyzedRelation, Collection<Integer>>(specs.size()),
            INT_SET_SUPPLIER::get);

        Integer idx = 0;
        List<QualifiedName> allRelations = new ArrayList<>();
        for (Symbol symbol : orderBy.orderBySymbols()) {
            // If order by is pointing to an aggregation we cannot push it down
            // because ordering needs to happen AFTER the join
            if (Aggregations.containsAggregation(symbol)) {
                continue;
            }
            relations.clear();
            FieldsVisitor.visitFields(symbol, relationsConsumer);

            if (relations.size() == 1) {
                AnalyzedRelation rel = Iterables.getOnlyElement(relations);
                if (!allRelations.contains(rel.getQualifiedName()) || allRelations.get(allRelations.size() - 1).equals(rel.getQualifiedName())) {
                    splits.put(rel, idx);
                    allRelations.add(rel.getQualifiedName());
                }
            } else {
                break;
            }
            idx++;
        }
        relations.clear();
        return splits;
    }

    private final static class JoinConditionValidator extends DefaultTraversalSymbolVisitor<Void, Symbol> {

        private static final JoinConditionValidator INSTANCE = new JoinConditionValidator();

        /**
         * @throws IllegalArgumentException thrown if the join condition is not valid
         */
        public static void validate(Symbol joinCondition) {
            if (joinCondition != null) {
                INSTANCE.process(joinCondition, null);
            }
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
            throw new IllegalArgumentException("Cannot use MATCH predicates on columns of 2 different relations " +
                                               "if it cannot be logically applied on each of them separately");
        }
    }
}
