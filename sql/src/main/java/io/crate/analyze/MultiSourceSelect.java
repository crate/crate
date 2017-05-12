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

package io.crate.analyze;

import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Sets2;
import io.crate.metadata.Functions;
import io.crate.metadata.Path;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

public class MultiSourceSelect implements QueriedRelation {

    private final Map<QualifiedName, AnalyzedRelation> sources;
    private final Fields fields;
    private final List<JoinPair> joinPairs;
    private final Set<Symbol> requiredForQuery;
    private final Set<Field> canBeFetched;
    private final Optional<RemainingOrderBy> remainingOrderBy;
    private QualifiedName qualifiedName;
    private QuerySpec querySpec;

    /**
     * Create a new MultiSourceSelect which has it's sources transformed into subQueries which contain
     * any expressions that only affect a single source.
     * Example:
     * <pre>
     *     select t1.x, t2.y from t1, t2 where t1.x = 10
     *
     *     becomes
     *
     *     select t1.x, t2.y from
     *          (select x from t1 where x = 10) t1,
     *          (select y from t2)
     * </pre>
     */
    public static MultiSourceSelect createWithPushDown(RelationNormalizer relationNormalizer,
                                                       Functions functions,
                                                       TransactionContext transactionContext,
                                                       MultiSourceSelect mss,
                                                       QuerySpec querySpec) {
        RelationSplitter splitter = new RelationSplitter(
            querySpec,
            mss.sources.values(),
            mss.joinPairs
        );
        splitter.process();

        Function<Field, Field> convertFieldToPointToNewRelations = Function.identity();
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : mss.sources.entrySet()) {
            AnalyzedRelation relation = entry.getValue();
            QuerySpec spec = splitter.getSpec(relation);
            QueriedRelation queriedRelation = Relations.applyQSToRelation(relationNormalizer, functions, transactionContext, relation, spec);
            Function<Field, Field> convertField = f -> mapFieldToNewRelation(f, relation, queriedRelation);
            querySpec = querySpec.copyAndReplace(FieldReplacer.bind(convertField));
            entry.setValue(queriedRelation);

            convertFieldToPointToNewRelations = convertFieldToPointToNewRelations.andThen(convertField);
        }
        Function<? super Symbol, ? extends Symbol> convertFieldInSymbolsToNewRelations =
            FieldReplacer.bind(convertFieldToPointToNewRelations);

        if (splitter.remainingOrderBy().isPresent()) {
            splitter.remainingOrderBy().get().orderBy().replace(convertFieldInSymbolsToNewRelations);
        }
        for (JoinPair joinPair : mss.joinPairs) {
            joinPair.replaceCondition(convertFieldInSymbolsToNewRelations);
        }
        Set<Symbol> requiredForQuery = Sets2.transformedCopy(splitter.requiredForQuery(), convertFieldInSymbolsToNewRelations);
        Set<Field> canBeFetched = Sets2.transformedCopy(splitter.canBeFetched(), convertFieldToPointToNewRelations);
        return new MultiSourceSelect(
            mss.sources(),
            mss.fields(),
            querySpec,
            mss.joinPairs,
            requiredForQuery,
            canBeFetched,
            splitter.remainingOrderBy()
        );
    }


    private static Field mapFieldToNewRelation(Field f, AnalyzedRelation oldRelation, QueriedRelation newRelation) {
        if (f.relation().equals(oldRelation)) {
            Field field = newRelation.getField(f.path(), Operation.READ);
            assert field != null : "Must be able to resolve field from injected relation: " + f;
            return field;
        }
        return f;
    }

    public MultiSourceSelect(Map<QualifiedName, AnalyzedRelation> sources,
                             Collection<? extends Path> outputNames,
                             QuerySpec querySpec,
                             List<JoinPair> joinPairs) {
        assert sources.size() > 1 : "MultiSourceSelect requires at least 2 relations";
        this.sources = sources;
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : sources.entrySet()) {
            entry.getValue().setQualifiedName(entry.getKey());
        }
        this.querySpec = querySpec;
        this.joinPairs = joinPairs;
        assert outputNames.size() == querySpec.outputs().size() : "size of outputNames and outputSymbols must match";
        fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (Path path : outputNames) {
            fields.add(path, new Field(this, path, outputsIterator.next().valueType()));
        }
        this.requiredForQuery = Collections.emptySet();
        this.canBeFetched = Collections.emptySet();
        this.remainingOrderBy = Optional.empty();
    }

    private MultiSourceSelect(Map<QualifiedName, AnalyzedRelation> sources,
                              Collection<Field> fields,
                              QuerySpec querySpec,
                              List<JoinPair> joinPairs,
                              Set<Symbol> requiredForQuery,
                              Set<Field> canBeFetched,
                              Optional<RemainingOrderBy> remainingOrderBy) {
        this.sources = sources;
        this.joinPairs = joinPairs;
        this.querySpec = querySpec;
        this.fields = new Fields(fields.size());
        for (Field field : fields) {
            this.fields.add(field.path(), new Field(this, field.path(), field.valueType()));
        }
        this.requiredForQuery = requiredForQuery;
        this.canBeFetched = canBeFetched;
        this.remainingOrderBy = remainingOrderBy;
    }

    public Set<Symbol> requiredForQuery() {
        return requiredForQuery;
    }

    public Set<Field> canBeFetched() {
        return canBeFetched;
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
        return sources;
    }

    public List<JoinPair> joinPairs() {
        return joinPairs;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitMultiSourceSelect(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on MultiSourceSelect is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }


    public Optional<RemainingOrderBy> remainingOrderBy() {
        return remainingOrderBy;
    }

    @Override
    public String toString() {
        return "MSS{" + sources.keySet() + '}';
    }

}
