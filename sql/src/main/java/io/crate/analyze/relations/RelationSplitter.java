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

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.fetch.FetchFieldExtractor;
import io.crate.analyze.symbol.*;
import io.crate.operation.operator.AndOperator;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.*;

public class RelationSplitter {

    private final QuerySpec querySpec;
    private final Set<Symbol> requiredForQuery = new HashSet<>();
    private final Map<QualifiedName, AnalyzedRelation> relations;
    private Set<Field> canBeFetched;

    private static final Supplier<Set<Integer>> INT_SET_SUPPLIER = new Supplier<Set<Integer>>() {
        @Override
        public Set<Integer> get() {
            return new HashSet<>();
        }
    };

    private Optional<OrderBy> remainingOrderBy = Optional.absent();
    private final Map<AnalyzedRelation, QuerySpec> specs;

    public static RelationSplitter process(QuerySpec querySpec, Collection<? extends AnalyzedRelation> relations) {
        RelationSplitter splitter = new RelationSplitter(querySpec, relations);
        splitter.process();
        return splitter;
    }

    private RelationSplitter(QuerySpec querySpec, Collection<? extends AnalyzedRelation> relations) {
        this.querySpec = querySpec;
        specs = new IdentityHashMap<>(relations.size());
        this.relations = new HashMap<>(relations.size());
        for (AnalyzedRelation relation : relations) {
            specs.put(relation, new QuerySpec());
            this.relations.put(relation.getQualifiedName(), relation);
        }
    }

    public Optional<OrderBy> remainingOrderBy() {
        return remainingOrderBy;
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

    private void process() {
        processOrderBy();
        processWhere();
        processOutputs();
    }

    private QuerySpec getSpec(QualifiedName relationName) {
        return specs.get(relations.get(relationName));
    }

    private void processOutputs() {
        FieldCollectingVisitor.Context context = new FieldCollectingVisitor.Context(specs.size());

        // declare all symbols from the remainging order by as required for query
        if (remainingOrderBy.isPresent()) {
            requiredForQuery.addAll(remainingOrderBy.get().orderBySymbols());
            // we need to add also the used symbols for query phase
            FieldCollectingVisitor.INSTANCE.process(remainingOrderBy.get().orderBySymbols(), context);
        }

        if (querySpec.where().hasQuery()) {
            FieldCollectingVisitor.INSTANCE.process(querySpec.where().query(), context);
        }

        // set the limit and offset where possible
        if (querySpec.limit().isPresent()) {
            for (AnalyzedRelation rel : Sets.difference(specs.keySet(), context.fields.keySet())) {
                QuerySpec spec = specs.get(rel);
                spec.limit(querySpec.limit().get() + querySpec.offset());
            }
        }

        // add all order by symbols to context outputs
        for (Map.Entry<AnalyzedRelation, QuerySpec> entry : specs.entrySet()) {
            if (entry.getValue().orderBy().isPresent()){
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
            assert entry.getValue().outputs() == null;
            entry.getValue().outputs(new ArrayList<>(fields));
        }
    }

    private void processWhere() {
        if (!querySpec.where().hasQuery()) {
            return;
        }
        Symbol query = querySpec.where().query();
        assert query != null;
        QuerySplittingVisitor.Context context = QuerySplittingVisitor.INSTANCE.process(querySpec.where().query());
        JoinConditionValidator.INSTANCE.process(context.query(), null);
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
        Multimap<AnalyzedRelation, Integer> splits = Multimaps.newSetMultimap(
                new IdentityHashMap<AnalyzedRelation, Collection<Integer>>(specs.size()),
                INT_SET_SUPPLIER);
        Integer idx = 0;
        for (Symbol symbol : orderBy.orderBySymbols()) {
            relations.clear();
            RelationCounter.INSTANCE.process(symbol, relations);
            if (relations.size() == 1) {
                splits.put(Iterables.getOnlyElement(relations), idx);
            } else {
                // not pushed down
                splits.put(null, idx);
            }
            idx++;
        }
        for (Map.Entry<AnalyzedRelation, Collection<Integer>> entry : splits.asMap().entrySet()) {
            OrderBy newOrderBy = orderBy.subset(entry.getValue());
            AnalyzedRelation relation = entry.getKey();
            if (relation == null) {
                remainingOrderBy = Optional.of(newOrderBy);
            } else {
                QuerySpec spec = getSpec(relation);
                assert !spec.orderBy().isPresent();
                spec.orderBy(newOrderBy);
                requiredForQuery.addAll(newOrderBy.orderBySymbols());
            }
        }
    }

    public static class RelationCounter extends DefaultTraversalSymbolVisitor<Set<AnalyzedRelation>, Void> {

        static final RelationCounter INSTANCE = new RelationCounter();

        @Override
        public Void visitField(Field field, Set<AnalyzedRelation> context) {
            context.add(field.relation());
            return super.visitField(field, context);
        }
    }

    private static class JoinConditionValidator extends SymbolVisitor<Void, Symbol> {

        private static final JoinConditionValidator INSTANCE = new JoinConditionValidator();

        @Override
        public Symbol process(Symbol symbol, @Nullable Void context) {
            return symbol != null ? super.process(symbol, context) : null;
        }

        @Override
        public Symbol visitFunction(Function symbol, Void context) {
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return null;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
            throw new IllegalArgumentException("Cannot use MATCH predicates on columns of 2 different relations " +
                                               "if it cannot be logically applied on each of them separately");
        }
    }
}
