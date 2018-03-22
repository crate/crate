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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.sql.tree.ParameterExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class InsertFromSubQueryAnalyzer {

    private final Functions functions;
    private final Schemas schemas;
    private final RelationAnalyzer relationAnalyzer;


    private static class ValuesResolver implements ValuesAwareExpressionAnalyzer.ValuesResolver {

        private final DocTableRelation targetTableRelation;
        private final List<Reference> targetColumns;

        ValuesResolver(DocTableRelation targetTableRelation, List<Reference> targetColumns) {
            this.targetTableRelation = targetTableRelation;
            this.targetColumns = targetColumns;
        }

        @Override
        public Symbol allocateAndResolve(Field argumentColumn) {
            Reference reference = targetTableRelation.resolveField(argumentColumn);
            int i = targetColumns.indexOf(reference);
            if (i < 0) {
                throw new IllegalArgumentException(SymbolFormatter.format(
                    "Column '%s' that is used in the VALUES() expression is not part of the target column list",
                    argumentColumn));
            }
            assert reference != null : "reference must not be null";
            return new InputColumn(i, argumentColumn.valueType());
        }
    }

    InsertFromSubQueryAnalyzer(Functions functions, Schemas schemas, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.schemas = schemas;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedInsertStatement analyze(InsertFromSubquery insert, ParamTypeHints typeHints, TransactionContext txnCtx) {
        TableIdent tableIdent = TableIdent.of(insert.table(), txnCtx.sessionContext().defaultSchema());
        DocTableInfo targetTable = schemas.getTableInfo(tableIdent, Operation.INSERT);
        DocTableRelation tableRelation = new DocTableRelation(targetTable);

        QueriedRelation subQueryRelation =
            (QueriedRelation) relationAnalyzer.analyzeUnbound(insert.subQuery(), txnCtx, typeHints);

        List<Reference> targetColumns =
            new ArrayList<>(resolveTargetColumns(insert.columns(), targetTable, subQueryRelation.fields().size()));
        validateColumnsAndAddCastsIfNecessary(targetColumns, subQueryRelation.outputs());

        Map<Reference, Symbol> onDuplicateKeyAssignments = processUpdateAssignments(
            tableRelation,
            targetColumns,
            typeHints,
            txnCtx,
            new NameFieldProvider(tableRelation),
            insert.onDuplicateKeyAssignments()
        );

        return new AnalyzedInsertStatement(subQueryRelation, onDuplicateKeyAssignments);
    }

    public AnalyzedStatement analyze(InsertFromSubquery node, Analysis analysis) {
        DocTableInfo tableInfo = schemas.getTableInfo(
            TableIdent.of(node.table(), analysis.sessionContext().defaultSchema()),
            Operation.INSERT);

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        FieldProvider fieldProvider = new NameFieldProvider(tableRelation);

        QueriedRelation source = (QueriedRelation) relationAnalyzer.analyze(
            node.subQuery(), analysis.transactionContext(), analysis.parameterContext());

        List<Reference> targetColumns = new ArrayList<>(resolveTargetColumns(node.columns(), tableInfo, source.fields().size()));
        validateColumnsAndAddCastsIfNecessary(targetColumns, source.outputs());

        Map<Reference, Symbol> onDuplicateKeyAssignments = processUpdateAssignments(
            tableRelation,
            targetColumns,
            analysis.parameterContext(),
            analysis.transactionContext(),
            fieldProvider,
            node.onDuplicateKeyAssignments()
        );

        return new InsertFromSubQueryAnalyzedStatement(
            source,
            tableInfo,
            targetColumns,
            onDuplicateKeyAssignments);
    }

    static Collection<Reference> resolveTargetColumns(Collection<String> targetColumnNames,
                                                      DocTableInfo targetTable,
                                                      int numSourceColumns) {
        if (targetColumnNames.isEmpty()) {
            return targetColumnsFromTargetTable(targetTable, numSourceColumns);
        }
        LinkedHashSet<Reference> columns = new LinkedHashSet<>(targetColumnNames.size());
        for (String targetColumnName : targetColumnNames) {
            ColumnIdent columnIdent = new ColumnIdent(targetColumnName);
            Reference reference = targetTable.getReference(columnIdent);
            Reference targetReference;
            if (reference == null) {
                DynamicReference dynamicReference = targetTable.getDynamic(columnIdent, true);
                if (dynamicReference == null) {
                    throw new ColumnUnknownException(targetColumnName, targetTable.ident());
                }
                targetReference = dynamicReference;
            } else {
                targetReference = reference;
            }
            if (!columns.add(targetReference)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", targetColumnName));
            }
        }
        return columns;
    }


    private static Collection<Reference> targetColumnsFromTargetTable(DocTableInfo targetTable, int numSourceColumns) {
        List<Reference> columns = new ArrayList<>(targetTable.columns().size());
        int idx = 0;
        for (Reference reference : targetTable.columns()) {
            if (idx > numSourceColumns) {
                break;
            }
            columns.add(reference);
            idx++;
        }
        return columns;
    }

    /**
     * validate that result columns from subquery match explicit insert columns
     * or complete table schema
     */
    private static void validateColumnsAndAddCastsIfNecessary(List<Reference> targetColumns,
                                                              List<Symbol> sources) {
        if (targetColumns.size() != sources.size()) {
            Collector<CharSequence, ?, String> commaJoiner = Collectors.joining(", ");
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Number of target columns (%s) of insert statement doesn't match number of source columns (%s)",
                targetColumns.stream().map(r -> r.column().sqlFqn()).collect(commaJoiner),
                sources.stream().map(SymbolPrinter.INSTANCE::printUnqualified).collect(commaJoiner)));
        }

        for (int i = 0; i < targetColumns.size(); i++) {
            Reference targetCol = targetColumns.get(i);
            Symbol source = sources.get(i);
            DataType targetType = targetCol.valueType();
            if (targetType.id() == DataTypes.UNDEFINED.id() || source.valueType().isConvertableTo(targetType)) {
                continue;
            }
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Type of subquery column %s (%s) does not match is not convertable to the type of table column %s (%s)",
                source,
                source.valueType(),
                targetCol.column().fqn(),
                targetType
            ));
        }
    }

    static Map<Reference, Symbol> getUpdateAssignments(Functions functions,
                                                       DocTableRelation targetTable,
                                                       List<Reference> targetCols,
                                                       ExpressionAnalyzer exprAnalyzer,
                                                       TransactionContext txnCtx,
                                                       Function<ParameterExpression, Symbol> paramConverter,
                                                       List<Assignment> assignments) {
        if (assignments.isEmpty()) {
            return Collections.emptyMap();
        }
        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext();
        ValuesResolver valuesResolver = new ValuesResolver(targetTable, targetCols);
        ValuesAwareExpressionAnalyzer valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
            functions, txnCtx, paramConverter, new NameFieldProvider(targetTable), valuesResolver);
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, targetTable);

        HashMap<Reference, Symbol> updateAssignments = new HashMap<>(assignments.size());
        for (Assignment assignment : assignments) {
            Reference targetCol = requireNonNull(
                targetTable.resolveField((Field) exprAnalyzer.convert(assignment.columnName(), exprCtx)),
                "resolveField must work on a field that was just resolved"
            );

            Symbol valueSymbol = ValueNormalizer.normalizeInputForReference(
                normalizer.normalize(valuesAwareExpressionAnalyzer.convert(assignment.expression(), exprCtx), txnCtx),
                targetCol,
                targetTable.tableInfo()
            );
            updateAssignments.put(targetCol, valueSymbol);
        }
        return updateAssignments;
    }

    private Map<Reference, Symbol> processUpdateAssignments(DocTableRelation tableRelation,
                                                            List<Reference> targetColumns,
                                                            java.util.function.Function<ParameterExpression, Symbol> parameterContext,
                                                            TransactionContext transactionContext,
                                                            FieldProvider fieldProvider,
                                                            List<Assignment> assignments) {
        if (assignments.isEmpty()) {
            return Collections.emptyMap();
        }

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, transactionContext, parameterContext, fieldProvider, null, Operation.UPDATE);

        return getUpdateAssignments(
            functions, tableRelation, targetColumns, expressionAnalyzer, transactionContext, parameterContext, assignments);
    }
}
