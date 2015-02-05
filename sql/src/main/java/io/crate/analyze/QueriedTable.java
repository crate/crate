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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.planner.consumer.QuerySplitter;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;

import java.util.*;

public class QueriedTable implements QueriedRelation {

    private final static SplittingVisitor SPLITTING_VISITOR = new SplittingVisitor();
    private final static FieldReplacingVisitor FIELD_REPLACING_VISITOR = new FieldReplacingVisitor();

    private final TableRelation tableRelation;
    private final QuerySpec querySpec;
    private final QualifiedName name;
    private final ArrayList<Field> fields;

    public QueriedTable(QualifiedName name, TableRelation tableRelation, List<OutputName> outputNames,
                        QuerySpec querySpec) {
        this.name = name;
        this.tableRelation = tableRelation;
        this.querySpec = querySpec;
        this.fields = new ArrayList<>(outputNames.size());
        for (int i = 0; i < outputNames.size(); i++) {
            fields.add(new Field(this, outputNames.get(i), querySpec.outputs().get(i).valueType()));
        }
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    public TableRelation tableRelation() {
        return tableRelation;
    }

    public QueriedRelation normalize(AnalysisMetaData analysisMetaData){
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(analysisMetaData, tableRelation, true);
        this.querySpec().normalize(normalizer);
        return this;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedTable(this, context);
    }

    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField on SelectAnalyzedStatement is not implemented");
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("SelectAnalyzedStatement is not writable");
    }

    @Override
    public List<Field> fields() {
        return fields;
    }


    public WhereClause consumeWhereClause(WhereClause clause) {
        return null;
    }

    public OrderBy consumeOrderBy(OrderBy orderBy) {
        return null;
    }

    /**
     * this will create a new QueriedTable for the given TableRelation
     *
     * It will walk through all symbols from QuerySpec and pull-down any symbols that the new QueriedTable can handle.
     *
     * The symbols that are pulled down from the original querySpec will be replaced with symbols
     * that point to a output/field of the QueriedTable.
     */
    public static QueriedTable newSubRelation(QualifiedName name, TableRelation tableRelation, QuerySpec querySpec) {
        QuerySpec splitQuerySpec = new QuerySpec();
        List<Symbol> outputs = querySpec.outputs();
        if (outputs == null || outputs.isEmpty()) {
            throw new IllegalArgumentException("a querySpec needs to have some outputs in order to create a new sub-relation");
        }
        splitQuerySpec.outputs(splitForRelation(tableRelation, outputs).splitSymbols());

        WhereClause where = querySpec.where();
        if (where != null && where.hasQuery()) {
            QuerySplitter.SplitQueries splitQueries = QuerySplitter.splitForRelation(tableRelation, where.query());
            if (splitQueries.relationQuery() != null) {
                splitQuerySpec.where(new WhereClause(splitQueries.relationQuery()));
                querySpec.where(new WhereClause(splitQueries.remainingQuery()));
            } else {
                splitQuerySpec.where(WhereClause.MATCH_ALL);
            }
        } else {
            splitQuerySpec.where(WhereClause.MATCH_ALL);
        }

        Integer limit = querySpec.limit();
        if (limit != null) {
            splitQuerySpec.limit(limit + querySpec.offset());
        }

        List<OutputName> outputNames = new ArrayList<>(splitQuerySpec.outputs().size());
        for (Symbol symbol : splitQuerySpec.outputs()) {
            outputNames.add(new OutputName(SymbolFormatter.format(symbol)));
        }

        QueriedTable queriedTable = new QueriedTable(name, tableRelation, outputNames, splitQuerySpec);
        Map<Symbol, Field> fieldMap = new HashMap<>();
        for (int i = 0; i < splitQuerySpec.outputs().size(); i++) {
            fieldMap.put(splitQuerySpec.outputs().get(i), queriedTable.fields().get(i));
        }
        replaceFields(querySpec.outputs(), fieldMap);
        return queriedTable;
    }


    private static void replaceFields(List<Symbol> outputs, Map<Symbol, Field> fieldMap) {
        for (int i = 0; i < outputs.size(); i++) {
            outputs.set(i, FIELD_REPLACING_VISITOR.process(outputs.get(i), fieldMap));
        }
    }

    private static class FieldReplacingVisitor extends SymbolVisitor<Map<Symbol, Field>, Symbol> {
        @Override
        public Symbol visitFunction(Function symbol, Map<Symbol, Field> context) {
            Field field = context.get(symbol);
            if (field != null) {
                return field;
            }
            if (symbol.arguments().isEmpty()) {
                return symbol;
            }
            List<Symbol> newArgs = new ArrayList<>(symbol.arguments().size());
            for (Symbol argument : symbol.arguments()) {
                newArgs.add(process(argument, context));
            }
            return new Function(symbol.info(), newArgs);
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Map<Symbol, Field> context) {
            Field field = context.get(symbol);
            if (field != null) {
                return field;
            }
            return symbol;
        }
    }

    private static class SplitContext {
        final AnalyzedRelation relation;
        final List<Symbol> splitSymbols = new ArrayList<>();
        final Stack<Symbol> parents = new Stack<>();

        public SplitContext(AnalyzedRelation relation) {
            this.relation = relation;
        }

        public List<Symbol> splitSymbols() {
            return splitSymbols;
        }
    }

    private static SplitContext splitForRelation(AnalyzedRelation relation, Collection<? extends Symbol> symbols) {
        SplitContext context = new SplitContext(relation);
        for (Symbol symbol : symbols) {
            SPLITTING_VISITOR.process(symbol, context);
        }
        return context;
    }

    private static class SplittingVisitor extends SymbolVisitor<SplitContext, Symbol> {

        @Override
        public Symbol visitFunction(Function function, SplitContext context) {
            List<Symbol> newArgs = new ArrayList<>(function.arguments().size());
            context.parents.push(function);
            for (Symbol argument : function.arguments()) {
                Symbol processed = process(argument, context);
                if (processed == null) {
                    continue;
                }
                newArgs.add(processed);
            }
            context.parents.pop();

            if (newArgs.size() == function.arguments().size()) {
                Function newFunction = new Function(function.info(), newArgs);
                if (context.parents.isEmpty()) {
                    context.splitSymbols.add(newFunction);
                }
                return newFunction;
            } else {
                newArg:
                for (Symbol newArg : newArgs) {
                    if ( !(newArg instanceof Function) || ((Function) newArg).info().deterministic()) {
                        for (Symbol symbol : context.splitSymbols) {
                            if (symbol.equals(newArg)) {
                                break newArg;
                            }
                        }
                    }
                    context.splitSymbols.add(newArg);
                }
            }
            return null;
        }

        @Override
        public Symbol visitLiteral(Literal symbol, SplitContext context) {
            return symbol;
        }

        @Override
        public Symbol visitField(Field field, SplitContext context) {
            if (field.relation() == context.relation) {
                if (context.parents.isEmpty()) {
                    context.splitSymbols.add(field);
                }
                return field;
            }
            return null;
        }
    }
}
