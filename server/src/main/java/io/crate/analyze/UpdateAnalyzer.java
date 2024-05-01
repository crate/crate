/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.expression.symbol.Symbols.unwrapReferenceFromCast;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.scalar.ArraySetFunction;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Update;
import io.crate.types.ArrayType;
import io.crate.types.IntegerType;
import io.crate.types.ObjectType;

/**
 * Used to analyze statements like: `UPDATE t1 SET col1 = ? WHERE id = ?`
 */
public final class UpdateAnalyzer {

    private static final Predicate<Reference> IS_OBJECT_ARRAY = input ->
        input != null &&
        input.valueType() instanceof ArrayType<?> arrayType &&
        arrayType.innerType().id() == ObjectType.ID;

    private final NodeContext nodeCtx;
    private final RelationAnalyzer relationAnalyzer;

    UpdateAnalyzer(NodeContext nodeCtx, RelationAnalyzer relationAnalyzer) {
        this.nodeCtx = nodeCtx;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedUpdateStatement analyze(Update update, ParamTypeHints typeHints, CoordinatorTxnCtx txnCtx) {
        /* UPDATE t1 SET col1 = ?, col2 = ? WHERE id = ?`
         *               ^^^^^^^^^^^^^^^^^^       ^^^^^^
         *               assignments               whereClause
         *
         *               col1 = ?
         *               |      |
         *               |     source
         *             columnName/target
         */
        StatementAnalysisContext stmtCtx = new StatementAnalysisContext(typeHints, Operation.UPDATE, txnCtx);
        final RelationAnalysisContext relCtx = stmtCtx.startRelation();
        AnalyzedRelation relation = relationAnalyzer.analyze(update.relation(), stmtCtx);
        stmtCtx.endRelation();

        MaybeAliasedStatement maybeAliasedStatement = MaybeAliasedStatement.analyze(relation);
        relation = maybeAliasedStatement.nonAliasedRelation();

        if (!(relation instanceof AbstractTableRelation)) {
            throw new UnsupportedOperationException("UPDATE is only supported on base-tables");
        }
        AbstractTableRelation<?> table = (AbstractTableRelation<?>) relation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.CLUSTER,
            null,
            table,
            f -> f.signature().isDeterministic());
        SubqueryAnalyzer subqueryAnalyzer =
            new SubqueryAnalyzer(relationAnalyzer, new StatementAnalysisContext(typeHints, Operation.READ, txnCtx));

        ExpressionAnalyzer sourceExprAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            typeHints,
            new FullQualifiedNameFieldProvider(
                relCtx.sources(),
                relCtx.parentSources(),
                txnCtx.sessionSettings().searchPath().currentSchema()
            ),
            subqueryAnalyzer
        );
        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        LinkedHashMap<Reference, Symbol> assignmentByTargetCol = getAssignments(
            update.assignments(), typeHints, txnCtx, table, normalizer, subqueryAnalyzer, sourceExprAnalyzer, exprCtx);

        Symbol query = Objects.requireNonNullElse(
            sourceExprAnalyzer.generateQuerySymbol(update.whereClause(), exprCtx),
            Literal.BOOLEAN_TRUE
        );
        query = maybeAliasedStatement.maybeMapFields(query);

        Symbol normalizedQuery = normalizer.normalize(query, txnCtx);
        SelectAnalysis selectAnalysis = SelectAnalyzer.analyzeSelectItems(
            update.returningClause(),
            relCtx.sources(),
            sourceExprAnalyzer,
            exprCtx
        );
        List<Symbol> outputSymbol = selectAnalysis.outputSymbols();
        return new AnalyzedUpdateStatement(
            table,
            assignmentByTargetCol,
            normalizedQuery,
            outputSymbol.isEmpty() ? null : outputSymbol
        );
    }

    private LinkedHashMap<Reference, Symbol> getAssignments(List<Assignment<Expression>> assignments,
                                                            ParamTypeHints typeHints,
                                                            CoordinatorTxnCtx txnCtx,
                                                            AbstractTableRelation<?> table,
                                                            EvaluatingNormalizer normalizer,
                                                            SubqueryAnalyzer subqueryAnalyzer,
                                                            ExpressionAnalyzer sourceExprAnalyzer,
                                                            ExpressionAnalysisContext exprCtx) {
        LinkedHashMap<Reference, Symbol> assignmentByTargetCol = new LinkedHashMap<>();
        ExpressionAnalyzer targetExprAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            typeHints,
            new NameFieldProvider(table),
            subqueryAnalyzer,
            Operation.UPDATE
        );
        assert assignments instanceof RandomAccess
            : "assignments should implement RandomAccess for indexed loop to avoid iterator allocations";
        TableInfo tableInfo = table.tableInfo();
        ArraySetFunctionAllocator arraySetFunctionAllocator =
            new ArraySetFunctionAllocator((func, args) -> sourceExprAnalyzer.allocateFunction(func, args, exprCtx));
        for (int i = 0; i < assignments.size(); i++) {
            Assignment<Expression> assignment = assignments.get(i);
            Symbol target = normalizer.normalize(targetExprAnalyzer.convert(assignment.columnName(), exprCtx), txnCtx);
            if (target instanceof Reference targetCol) {
                rejectUpdatesToFieldsOfObjectArrays(tableInfo, targetCol, IS_OBJECT_ARRAY);

                Symbol source = ValueNormalizer.normalizeInputForReference(
                    normalizer.normalize(sourceExprAnalyzer.convert(assignment.expression(), exprCtx), txnCtx),
                    targetCol,
                    tableInfo,
                    s -> normalizer.normalize(s, txnCtx)
                );

                if (assignmentByTargetCol.put(targetCol, source) != null) {
                    throw new IllegalArgumentException("Target expression repeated: " + targetCol.column().sqlFqn());
                }
                continue;
            } else if (target instanceof Function function && SubscriptFunction.NAME.equals(function.name())) {
                var args = function.arguments();
                Symbol baseCol = args.get(0);
                Symbol indexToUpdate = args.get(1);

                if (baseCol instanceof Reference targetCol) {
                    rejectUpdatesToFieldsOfObjectArrays(tableInfo, targetCol, IS_OBJECT_ARRAY);
                    assert targetCol.valueType() instanceof ArrayType<?> : "targetCol should be an array type.";
                    assert indexToUpdate.valueType() instanceof IntegerType : "indexToUpdate should be an integer type.";

                    SimpleReference arrayElementRefForValueNormalization =
                        new SimpleReference(
                            targetCol.ident(),
                            targetCol.granularity(),
                            ((ArrayType<?>) targetCol.valueType()).innerType(),
                            targetCol.columnPolicy(),
                            targetCol.indexType(),
                            targetCol.isNullable(),
                            targetCol.hasDocValues(),
                            targetCol.position(),
                            targetCol.oid(),
                            targetCol.isDropped(),
                            targetCol.defaultExpression()
                        );

                    Symbol targetValue = ValueNormalizer.normalizeInputForReference(
                        normalizer.normalize(sourceExprAnalyzer.convert(assignment.expression(), exprCtx), txnCtx),
                        arrayElementRefForValueNormalization,
                        tableInfo,
                        s -> normalizer.normalize(s, txnCtx));

                    arraySetFunctionAllocator.put(targetCol, indexToUpdate, targetValue);
                    continue;
                } else if (unwrapReferenceFromCast(baseCol) instanceof Reference targetCol) {
                    rejectUpdatesToFieldsOfObjectArrays(tableInfo, targetCol, IS_OBJECT_ARRAY);
                }
            }
            throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "cannot use expression %s as a left side of an assignment",
                    assignment.columnName()));
        }
        for (var e : arraySetFunctionAllocator.allocate().entrySet()) {
            if (assignmentByTargetCol.put(e.getKey(), e.getValue()) != null) {
                throw new IllegalArgumentException("Target expression repeated: " + e.getKey().column().sqlFqn());
            }
        }
        return assignmentByTargetCol;
    }

    private static void rejectUpdatesToFieldsOfObjectArrays(TableInfo tableInfo, Reference info, Predicate<Reference> parentMatchPredicate) {
        for (var parent : tableInfo.getParents(info.column())) {
            if (parentMatchPredicate.test(parent)) {
                throw new IllegalArgumentException("Updating fields of object arrays is not supported");
            }
        }
    }

    private static class ArraySetFunctionAllocator {
        private final Map<Reference, LinkedHashMap<Symbol, Symbol>> mappings;
        private final BiFunction<String, List<Symbol>, Symbol> allocateFunction;

        public ArraySetFunctionAllocator(BiFunction<String, List<Symbol>, Symbol> allocateFunction) {
            this.allocateFunction = allocateFunction;
            this.mappings = new HashMap<>();
        }

        public void put(Reference reference, Symbol index, Symbol value) {
            var mapping = mappings.get(reference);
            if (mapping != null) {
                mapping.put(index, value);
            } else {
                mapping = new LinkedHashMap<>();
                mapping.put(index, value);
                mappings.put(reference, mapping);
            }
        }

        public Map<Reference, Symbol> allocate() {
            Map<Reference, Symbol> refToArraySetMap = new HashMap<>(mappings.size());
            for (var e : mappings.entrySet()) {
                Reference targetCol = e.getKey();
                LinkedHashMap<Symbol, Symbol> mapping = e.getValue();
                refToArraySetMap.put(
                    targetCol,
                    allocateFunction.apply(
                        ArraySetFunction.NAME,
                        List.of(
                            targetCol,
                            allocateFunction.apply(ArrayFunction.NAME, mapping.keySet().stream().toList()),
                            allocateFunction.apply(ArrayFunction.NAME, mapping.values().stream().toList())
                        )
                    )
                );
            }
            return refToArraySetMap;
        }
    }
}
