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

import com.google.common.collect.Sets;
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

public final class RelationSplitter {

    private final QuerySpec querySpec;
    private final Set<Symbol> requiredForQuery = new HashSet<>();
    private final Map<AnalyzedRelation, QuerySpec> specs;
    private final Map<QualifiedName, AnalyzedRelation> relations;
    private final List<JoinPair> joinPairs;
    private final List<Symbol> joinConditions;
    private Set<Field> canBeFetched;
    private RemainingOrderBy remainingOrderBy;

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
        FieldCollectingVisitor.Context context = new FieldCollectingVisitor.Context(specs.size());

        // declare all symbols from the remaining order by as required for query
        if (remainingOrderBy != null) {
            OrderBy orderBy = remainingOrderBy.orderBy();
            requiredForQuery.addAll(orderBy.orderBySymbols());
            // we need to add also the used symbols for query phase
            FieldCollectingVisitor.INSTANCE.process(orderBy.orderBySymbols(), context);
        }

        if (querySpec.where().hasQuery()) {
            FieldCollectingVisitor.INSTANCE.process(querySpec.where().query(), context);
        }

        // collect all fields from all join conditions
        FieldCollectingVisitor.INSTANCE.process(joinConditions, context);

        // set the limit and offset where possible
        Optional<Symbol> limit = querySpec.limit();
        if (limit.isPresent()) {
            Optional<Symbol> limitAndOffset = Limits.mergeAdd(limit, querySpec.offset());
            for (AnalyzedRelation rel : Sets.difference(specs.keySet(), context.fields.keySet())) {
                QuerySpec spec = specs.get(rel);
                spec.limit(limitAndOffset);
            }
        }

        // add all order by symbols to context outputs
        for (Map.Entry<AnalyzedRelation, QuerySpec> entry : specs.entrySet()) {
            if (entry.getValue().orderBy().isPresent()) {
                context.fields.putAll(entry.getKey(), entry.getValue().orderBy().get().orderBySymbols());
            }
        }

        // everything except the actual outputs is required for query
        requiredForQuery.addAll(context.fields.values());

        // capture items from the outputs
        canBeFetched = FetchFieldExtractor.process(querySpec.outputs(), context.fields);

        FieldCollectingVisitor.INSTANCE.process(querySpec.outputs(), context);

        // generate the outputs of the subSpecs
        for (Map.Entry<AnalyzedRelation, QuerySpec> entry : specs.entrySet()) {
            Collection<Symbol> fields = context.fields.get(entry.getKey());
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

    /**
     * Move ORDER BY expressions to the subRelations if it's safe.
     * Move is safe if all order by expressions refer to the same relation and there is no outer join.
     *
     *  - NL with outer-join injects null rows; so it doesn't preserve the ordering
     *  - Two or more relations in ORDER BY requires a post-join sorting, relying on preserving the pre-ordering doesn't work:
     *
     *  Example:
     *
     * <pre>
     *   ORDER BY tx, ty
     *
     *   tx = [1, 1, 2, 2]
     *   ty = [1, 2]
     *
     *   for x in tx:
     *      for y in ty:
     *
     *   results in
     *     1| 1
     *     1| 2
     *     1| 1
     *     1| 2
     *     ...
     *
     *   but should result in
     *
     *     1| 1
     *     1| 1
     *     1| 2
     *     1| 2
     *
     * </pre>
     */
    private void processOrderBy() {
        Optional<OrderBy> optOrderBy = querySpec.orderBy();
        if (!optOrderBy.isPresent()) {
            return;
        }
        OrderBy orderBy = optOrderBy.get();
        Set<AnalyzedRelation> relations = Collections.newSetFromMap(new IdentityHashMap<AnalyzedRelation, Boolean>());
        Consumer<Field> gatherRelations = f -> relations.add(f.relation());

        if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
            return;
        }
        for (Symbol orderExpr : orderBy.orderBySymbols()) {
            FieldsVisitor.visitFields(orderExpr, gatherRelations);
        }
        if (relations.size() == 1 && joinPairs.stream().noneMatch(p -> p.joinType().isOuter())) {
            AnalyzedRelation relationInOrderBy = relations.iterator().next();
            QuerySpec spec = getSpec(relationInOrderBy);
            orderBy = orderBy.copyAndReplace(Symbols.DEEP_COPY::apply);
            spec.orderBy(orderBy);
            requiredForQuery.addAll(orderBy.orderBySymbols());
        } else {
            remainingOrderBy = new RemainingOrderBy();
            for (AnalyzedRelation relation : relations) {
                remainingOrderBy.addRelation(relation.getQualifiedName());
            }
            remainingOrderBy.addOrderBy(orderBy);
        }
    }

    static class RelationCounter extends DefaultTraversalSymbolVisitor<Set<AnalyzedRelation>, Void> {

        static final RelationCounter INSTANCE = new RelationCounter();

        @Override
        public Void visitField(Field field, Set<AnalyzedRelation> context) {
            context.add(field.relation());
            return super.visitField(field, context);
        }
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
