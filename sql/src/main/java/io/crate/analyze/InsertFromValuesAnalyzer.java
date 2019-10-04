/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.ExcludedFieldProvider;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.exceptions.ColumnValidationException;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.expression.ValueExtractors;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceToLiteralConverter;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.Insert.DuplicateKeyContext.Type;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.ValuesList;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.crate.analyze.InsertFromSubQueryAnalyzer.getUpdateAssignments;
import static io.crate.analyze.InsertFromSubQueryAnalyzer.resolveTargetColumns;

class InsertFromValuesAnalyzer extends AbstractInsertAnalyzer {

    private static class ValuesResolver implements io.crate.analyze.ValuesResolver {

        private final DocTableRelation tableRelation;
        public List<Reference> columns;
        public List<String> assignmentColumns;
        public Object[] insertValues;

        public ValuesResolver(DocTableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }

        @Override
        public Symbol allocateAndResolve(Field argumentColumn) {
            // use containsKey instead of checking result .get() for null because inserted value might actually be null
            Reference columnReference = tableRelation.resolveField(argumentColumn);
            if (!columns.contains(columnReference)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Referenced column '%s' isn't part of the column list of the INSERT statement",
                    argumentColumn.path().sqlFqn()));
            }
            assert columnReference != null : "columnReference must not be null";
            DataType returnType = columnReference.valueType();
            assignmentColumns.add(columnReference.column().fqn());
            return Literal.of(returnType, returnType.value(insertValues[columns.indexOf(columnReference)]));
        }
    }

    InsertFromValuesAnalyzer(Functions functions, Schemas schemas) {
        super(functions, schemas);
    }


    public AnalyzedInsertStatement analyze(InsertFromValues<Expression> insert, ParamTypeHints typeHints, CoordinatorTxnCtx txnCtx) {
        if (insert.valuesLists().isEmpty()) {
            throw new IllegalArgumentException("VALUES clause must not be empty");
        }

        DocTableInfo targetTable = (DocTableInfo) schemas.resolveTableInfo(
            insert.table().getName(),
            Operation.INSERT,
            txnCtx.sessionContext().user(),
            txnCtx.sessionContext().searchPath()
        );
        DocTableRelation tableRelation = new DocTableRelation(targetTable);

        ExpressionAnalyzer exprAnalyzer = new ExpressionAnalyzer(
            functions,
            txnCtx,
            typeHints,
            new NameFieldProvider(tableRelation),
            null,
            Operation.INSERT
        );
        ValuesList firstRow = insert.valuesLists().get(0);

        List<Reference> targetCols = ImmutableList.copyOf(
            resolveTargetColumns(insert.columns(), targetTable, firstRow.values().size()));
        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext();

        ArrayList<List<Symbol>> rows = new ArrayList<>();

        // VALUES (1, 2), (3, 4)
        //        ^^^^^^   |
        //        row     valueExpr
        for (ValuesList row : insert.valuesLists()) {
            List<Expression> values = row.values();
            List<Symbol> rowSymbols = new ArrayList<>(targetCols.size());
            for (int i = 0; i < targetCols.size(); i++) {
                Expression valueExpr = values.get(i);
                Symbol valueSymbol = exprAnalyzer.convert(valueExpr, exprCtx);
                Reference targetCol = targetCols.get(i);

                valueSymbol = ExpressionAnalyzer.cast(valueSymbol, targetCol.valueType());
                rowSymbols.add(valueSymbol);
            }
            rows.add(rowSymbols);
        }

        Map<Reference, Symbol> onDuplicateKeyAssignments = getUpdateAssignments(
            functions,
            tableRelation,
            targetCols,
            exprAnalyzer.copyForOperation(Operation.UPDATE),
            txnCtx,
            typeHints,
            insert.getDuplicateKeyContext());
        return new AnalyzedInsertStatement(rows, onDuplicateKeyAssignments);
    }

    public AnalyzedStatement analyze(InsertFromValues<Expression> node, Analysis analysis) {
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            node.table().getName(),
            Operation.INSERT,
            analysis.sessionContext().user(),
            analysis.sessionContext().searchPath());

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        ValuesResolver valuesResolver = new ValuesResolver(tableRelation);
        Insert.DuplicateKeyContext duplicateKeyContext = node.getDuplicateKeyContext();

        verifyOnConflictTargets(duplicateKeyContext, tableInfo);

        final FieldProvider fieldProvider;
        if (duplicateKeyContext.getType() == Type.ON_CONFLICT_DO_UPDATE_SET) {
            fieldProvider = new ExcludedFieldProvider(new NameFieldProvider(tableRelation), valuesResolver);
        } else {
            fieldProvider = new NameFieldProvider(tableRelation);
        }
        Function<ParameterExpression, Symbol> convertParamFunction = analysis.parameterContext();

        ExpressionAnalyzer valuesAwareExpressionAnalyzer = new ExpressionAnalyzer(
            functions,
            analysis.transactionContext(),
            convertParamFunction,
            fieldProvider,
            null
        );

        final boolean ignoreDuplicateKeys =
            duplicateKeyContext.getType() == Type.ON_CONFLICT_DO_NOTHING;

        InsertFromValuesAnalyzedStatement statement = new InsertFromValuesAnalyzedStatement(
            tableInfo, analysis.parameterContext().numBulkParams(), ignoreDuplicateKeys);
        handleInsertColumns(node, node.maxValuesLength(), statement);

        Set<Reference> allReferencedReferences = new HashSet<>();
        for (GeneratedReference reference : tableInfo.generatedColumns()) {
            allReferencedReferences.addAll(reference.referencedReferences());
        }
        ReferenceToLiteralConverter refToLiteral = new ReferenceToLiteralConverter(
            statement.columns(), allReferencedReferences);

        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.CLUSTER,
            null,
            tableRelation);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            analysis.transactionContext(),
            convertParamFunction,
            fieldProvider,
            null,
            Operation.UPDATE
        );
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        analyzeColumns(statement.tableInfo(), statement.columns());
        for (ValuesList valuesList : node.valuesLists()) {
            analyzeValues(
                tableRelation,
                normalizer,
                expressionAnalyzer,
                expressionAnalysisContext,
                analysis.transactionContext(),
                valuesResolver,
                valuesAwareExpressionAnalyzer,
                valuesList,
                duplicateKeyContext.getAssignments(),
                statement,
                analysis.parameterContext(),
                refToLiteral);
        }
        return statement;
    }

    private void analyzeColumns(DocTableInfo tableInfo, List<Reference> columns) {
        Collection<ColumnIdent> notUsedNonGeneratedColumns = TransportShardUpsertAction.getNotUsedNonGeneratedColumns(columns.toArray(new Reference[]{}), tableInfo);
        ConstraintsValidator.validateConstraintsForNotUsedColumns(notUsedNonGeneratedColumns, tableInfo);
    }

    private void validateValuesSize(List<Expression> values,
                                    InsertFromValuesAnalyzedStatement statement,
                                    DocTableRelation tableRelation) {
        final int numValues = values.size();
        final int numInsertColumns = statement.columns().size();

        if (numValues != numInsertColumns) {
            final boolean firstValues = statement.sourceMaps().isEmpty();
            final boolean noExtraColumnsToAdd = tableRelation.tableInfo().generatedColumns().isEmpty() &&
                                 tableRelation.tableInfo().defaultExpressionColumns().isEmpty();
            final int numAddedColumnsWithExpression = statement.numAddedColumnsWithExpression();

            if (firstValues
                || noExtraColumnsToAdd
                || numValues != numInsertColumns - numAddedColumnsWithExpression) {
                // do not fail here when numbers are different if:
                //  * we are not at the first values AND
                //  * we have either generated columns or columns with default expression in the table AND
                //  * we are only missing the columns with expression which will be added
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Invalid number of values: Got %d columns specified but %d values",
                    numInsertColumns - numAddedColumnsWithExpression, numValues));
            }
        }
    }

    private void analyzeValues(DocTableRelation tableRelation,
                               EvaluatingNormalizer normalizer,
                               ExpressionAnalyzer expressionAnalyzer,
                               ExpressionAnalysisContext expressionAnalysisContext,
                               CoordinatorTxnCtx coordinatorTxnCtx,
                               ValuesResolver valuesResolver,
                               ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                               ValuesList node,
                               List<Assignment<Expression>> onDuplicateKeyAssignments,
                               InsertFromValuesAnalyzedStatement statement,
                               ParameterContext parameterContext,
                               ReferenceToLiteralConverter refToLiteral) {
        validateValuesSize(node.values(), statement, tableRelation);

        try {
            DocTableInfo tableInfo = statement.tableInfo();
            int numPks = tableInfo.primaryKey().size();
            Function<List<String>, String> idFunction =
                Id.compileWithNullValidation(tableInfo.primaryKey(), tableInfo.clusteredBy());
            if (parameterContext.numBulkParams() > 0) {
                for (int i = 0; i < parameterContext.numBulkParams(); i++) {
                    parameterContext.setBulkIdx(i);
                    addValues(
                        tableRelation,
                        normalizer,
                        expressionAnalyzer,
                        expressionAnalysisContext,
                        coordinatorTxnCtx,
                        valuesResolver,
                        valuesAwareExpressionAnalyzer,
                        node,
                        onDuplicateKeyAssignments,
                        statement,
                        refToLiteral,
                        numPks,
                        idFunction,
                        i
                    );
                }
            } else {
                addValues(
                    tableRelation,
                    normalizer,
                    expressionAnalyzer,
                    expressionAnalysisContext,
                    coordinatorTxnCtx,
                    valuesResolver,
                    valuesAwareExpressionAnalyzer,
                    node,
                    onDuplicateKeyAssignments,
                    statement,
                    refToLiteral,
                    numPks,
                    idFunction,
                    -1
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // can't throw IOException directly because of visitor interface
        }
    }

    private void addValues(DocTableRelation tableRelation,
                           EvaluatingNormalizer normalizer,
                           ExpressionAnalyzer expressionAnalyzer,
                           ExpressionAnalysisContext expressionAnalysisContext,
                           CoordinatorTxnCtx coordinatorTxnCtx,
                           ValuesResolver valuesResolver,
                           ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                           ValuesList node,
                           List<Assignment<Expression>> onDuplicateKeyAssignments,
                           InsertFromValuesAnalyzedStatement context,
                           ReferenceToLiteralConverter refToLiteral,
                           int numPrimaryKeys,
                           Function<List<String>, String> idFunction,
                           int bulkIdx) throws IOException {
        DocTableInfo tableInfo = context.tableInfo();
        if (tableInfo.isPartitioned()) {
            context.newPartitionMap();
        }
        String[] primaryKeyValues = new String[numPrimaryKeys];
        String routingValue = null;
        List<ColumnIdent> primaryKey = tableInfo.primaryKey();
        Object[] insertValues = new Object[node.values().size()];

        List<Reference> columnsProcessed = new ArrayList<>(context.tableInfo().columns().size());
        for (int i = 0, valuesSize = node.values().size(); i < valuesSize; i++) {
            Reference column = context.columns().get(i);
            columnsProcessed.add(column);
            final ColumnIdent columnIdent = column.column();
            Expression expression = node.values().get(i);
            Symbol valuesSymbol = normalizer.normalize(
                expressionAnalyzer.convert(expression, expressionAnalysisContext),
                coordinatorTxnCtx);

            // implicit type conversion
            Object value;
            try {
                valuesSymbol = ValueNormalizer.normalizeInputForReference(valuesSymbol, column, tableRelation.tableInfo());
                value = ((Input) valuesSymbol).value();
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
                throw new ColumnValidationException(columnIdent.sqlFqn(), tableInfo.ident(), e);
            } catch (ClassCastException e) {
                // symbol is no Input
                throw new ColumnValidationException(columnIdent.name(), tableInfo.ident(),
                    SymbolFormatter.format("Invalid value '%s' in insert statement", valuesSymbol));
            }

            if (context.primaryKeyColumnIndices().contains(i)) {
                if (value == null) {
                    throw new IllegalArgumentException("Primary key value must not be NULL");
                }
                int idx = primaryKey.indexOf(columnIdent);
                if (idx < 0) {
                    // oh look, one or more nested primary keys!
                    assert value instanceof Map : "value must be instance of Map";
                    for (ColumnIdent pkIdent : primaryKey) {
                        if (!pkIdent.getRoot().equals(columnIdent)) {
                            continue;
                        }
                        int pkIdx = primaryKey.indexOf(pkIdent);
                        Object nestedValue = Maps.getByPath((Map) value, pkIdent.path());
                        addPrimaryKeyValue(pkIdx, nestedValue, primaryKeyValues);
                    }
                } else {
                    addPrimaryKeyValue(idx, value, primaryKeyValues);
                }
            }
            if (i == context.routingColumnIndex()) {
                routingValue = extractRoutingValue(columnIdent, value, context);
            }
            if (context.partitionedByIndices().contains(i)) {
                Object rest = processPartitionedByValues(columnIdent, value, context);
                if (rest != null) {
                    insertValues[i] = rest;
                }
            } else {
                insertValues[i] = value;
            }
        }

        if (!onDuplicateKeyAssignments.isEmpty()) {
            valuesResolver.insertValues = insertValues;
            valuesResolver.columns = context.columns();
            Symbol[] onDupKeyAssignments = new Symbol[onDuplicateKeyAssignments.size()];
            valuesResolver.assignmentColumns = new ArrayList<>(onDuplicateKeyAssignments.size());
            for (int i = 0; i < onDuplicateKeyAssignments.size(); i++) {
                Assignment<Expression> assignment = onDuplicateKeyAssignments.get(i);
                Reference columnName = tableRelation.resolveField(
                    (Field) expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext));
                assert columnName != null : "columnName must not be null";

                Symbol valueSymbol = normalizer.normalize(
                    valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext),
                    coordinatorTxnCtx);
                Symbol assignmentExpression = ValueNormalizer.normalizeInputForReference(valueSymbol, columnName,
                    tableRelation.tableInfo());
                onDupKeyAssignments[i] = assignmentExpression;

                if (valuesResolver.assignmentColumns.size() == i) {
                    valuesResolver.assignmentColumns.add(columnName.column().fqn());
                }
            }
            context.addOnDuplicateKeyAssignments(onDupKeyAssignments);
            context.addOnDuplicateKeyAssignmentsColumns(
                valuesResolver.assignmentColumns.toArray(new String[valuesResolver.assignmentColumns.size()]));
        }

        // process generated column expressions and add columns + values
        ColumnExpressionContext ctx = new ColumnExpressionContext(
            tableRelation,
            context,
            normalizer,
            coordinatorTxnCtx,
            refToLiteral,
            columnsProcessed,
            primaryKeyValues,
            insertValues,
            routingValue);
        processColumnsWithExpression(ctx);
        insertValues = ctx.insertValues;
        routingValue = ctx.routingValue;

        context.sourceMaps().add(insertValues);
        String id = idFunction.apply(Arrays.asList(primaryKeyValues));
        context.addIdAndRouting(id, routingValue);
        if (bulkIdx >= 0) {
            context.bulkIndices().add(bulkIdx);
        }
    }

    /**
     * Sets a primary key value at the correct index of the given array structure.
     * Values could be applied in an unordered way, so given the correct column index of the defined primary key
     * definition is very important here.
     */
    private void addPrimaryKeyValue(int index, Object value, String[] primaryKeyValues) {
        if (value == null) {
            throw new IllegalArgumentException("Primary key value must not be NULL");
        }
        assert primaryKeyValues.length > index : "Index of primary key value is greater than the array holding the values";
        primaryKeyValues[index] = value.toString();
    }

    private String extractRoutingValue(ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        ColumnIdent clusteredByIdent = context.tableInfo().clusteredBy();
        assert clusteredByIdent != null : "clusteredByIdent must not be null";
        if (columnValue != null && !columnIdent.equals(clusteredByIdent)) {
            // oh my gosh! A nested clustered by value!!!
            assert columnValue instanceof Map : "columnValue must be instance of Map";
            columnValue = Maps.getByPath((Map) columnValue, clusteredByIdent.path());
        }
        if (columnValue == null) {
            throw new IllegalArgumentException("Clustered by value must not be NULL");
        }
        return columnValue.toString();
    }

    private Object processPartitionedByValues(final ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        int idx = context.tableInfo().partitionedBy().indexOf(columnIdent);
        Map<String, String> partitionMap = context.currentPartitionMap();
        if (idx < 0) {
            if (columnValue == null) {
                return null;
            }
            assert columnValue instanceof Map : "columnValue must be instance of Map";
            Map<String, Object> mapValue = (Map<String, Object>) columnValue;
            // hmpf, one or more nested partitioned by columns

            for (ColumnIdent partitionIdent : context.tableInfo().partitionedBy()) {
                if (partitionIdent.getRoot().equals(columnIdent)) {
                    Object nestedValue = mapValue.remove(String.join(".", partitionIdent.path()));
                    if (nestedValue instanceof BytesRef) {
                        nestedValue = ((BytesRef) nestedValue).utf8ToString();
                    }
                    if (partitionMap != null) {
                        partitionMap.put(partitionIdent.fqn(), BytesRefs.toString(nestedValue));
                    }
                }
            }

            // put the rest into source
            return mapValue;
        } else if (partitionMap != null) {
            partitionMap.put(columnIdent.name(), BytesRefs.toString(columnValue));
            return columnValue;
        }
        return null;
    }

    private static class ColumnExpressionContext {

        private final DocTableRelation tableRelation;
        private final InsertFromValuesAnalyzedStatement analyzedStatement;
        private final ReferenceToLiteralConverter refToLiteral;
        private final CoordinatorTxnCtx coordinatorTxnCtx;
        private final String[] primaryKeyValues;
        private final EvaluatingNormalizer normalizer;
        private final List<Reference> alreadyProcessedColumns;

        private Object[] insertValues;
        @Nullable
        private String routingValue;


        private ColumnExpressionContext(DocTableRelation tableRelation,
                                        InsertFromValuesAnalyzedStatement analyzedStatement,
                                        EvaluatingNormalizer normalizer,
                                        CoordinatorTxnCtx coordinatorTxnCtx,
                                        ReferenceToLiteralConverter refToLiteral,
                                        List<Reference> alreadyProcessedColumns,
                                        String[] primaryKeyValues,
                                        Object[] insertValues,
                                        @Nullable String routingValue) {
            this.tableRelation = tableRelation;
            this.analyzedStatement = analyzedStatement;
            this.coordinatorTxnCtx = coordinatorTxnCtx;
            this.primaryKeyValues = primaryKeyValues;
            this.insertValues = insertValues;
            this.routingValue = routingValue;
            this.refToLiteral = refToLiteral;
            this.normalizer = normalizer;
            this.alreadyProcessedColumns = alreadyProcessedColumns;
            refToLiteral.values(insertValues);
        }
    }

    private void processColumnsWithExpression(ColumnExpressionContext context) {

        List<ColumnIdent> primaryKey = context.analyzedStatement.tableInfo().primaryKey();

        List<Reference> columnsWithExpessionToProcess = new ArrayList<>(context.analyzedStatement.tableInfo.defaultExpressionColumns());
        columnsWithExpessionToProcess.removeAll(context.alreadyProcessedColumns);
        columnsWithExpessionToProcess
            .forEach(r -> processExpressionForReference(r, r::defaultExpression, context, primaryKey));
        context.tableRelation.tableInfo().generatedColumns()
            .forEach(r -> processExpressionForReference(r, r::generatedExpression, context, primaryKey));
    }

    private void processExpressionForReference(Reference reference,
                                               Supplier<Symbol> expressionSupplier,
                                               ColumnExpressionContext context,
                                               List<ColumnIdent> primaryKey) {
        Symbol valueSymbol = RefReplacer.replaceRefs(expressionSupplier.get(), context.refToLiteral);
        valueSymbol = context.normalizer.normalize(valueSymbol, context.coordinatorTxnCtx);
        if (valueSymbol.symbolType() == SymbolType.LITERAL) {
            Object value = ((Input) valueSymbol).value();
            if (primaryKey.contains(reference.column())) {
                final int idx = primaryKey.indexOf(reference.column());
                addPrimaryKeyValue(idx, value, context.primaryKeyValues);
            }
            ColumnIdent routingColumn = context.analyzedStatement.tableInfo().clusteredBy();
            if (routingColumn != null && routingColumn.equals(reference.column())) {
                context.routingValue = extractRoutingValue(routingColumn, value, context.analyzedStatement);
            }
            if (context.tableRelation.tableInfo().isPartitioned()
                && context.tableRelation.tableInfo().partitionedByColumns().contains(reference)) {
                addGeneratedPartitionedColumnValue(reference.column(),
                                                   value,
                                                   context.analyzedStatement.currentPartitionMap());
            } else {
                context.insertValues = addExtraColumnValue(context.analyzedStatement,
                                                           reference,
                                                           value,
                                                           context.insertValues,
                                                           reference instanceof GeneratedReference);
            }
        }

    }

    private Object[] addExtraColumnValue(InsertFromValuesAnalyzedStatement context,
                                         Reference reference,
                                         Object value,
                                         Object[] insertValues,
                                         boolean isGeneratedExpression) {
        int idx = -1;
        for (int i = 0; i < context.columns.size(); i++) {
            Reference column = context.columns.get(i);
            if (column.equals(reference)) {
                idx = i;
                break;
            }
            if (!reference.column().isTopLevel() && column.column().equals(reference.column().getRoot())) {
                // INSERT INTO (obj) VALUES ({x=1}) -> must merge into the map without `obj` prefix
                ColumnIdent target = reference.column().shiftRight();
                assert target != null : "Column that is not topLevel must not be null after shiftRight()";
                Map insertValue = (Map) insertValues[i];
                if (ValueExtractors.fromMap(insertValue, target) == null) {
                    Maps.mergeInto(insertValue, target.name(), target.path(), value);
                }
                return insertValues;
            }
        }
        if (idx == -1) {
            // add column & value
            context.addColumnWithExpression(reference);
            int valuesIdx = insertValues.length;
            insertValues = Arrays.copyOf(insertValues, insertValues.length + 1);
            insertValues[valuesIdx] = value;
        } else if (insertValues.length <= idx) {
            // only add value
            insertValues = Arrays.copyOf(insertValues, idx + 1);
            insertValues[idx] = value;
        } else if (isGeneratedExpression && (
                       (insertValues[idx] == null && value != null) ||
                       (insertValues[idx] != null && !insertValues[idx].equals(value)))) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Given value %s for generated column does not match defined generated expression value %s",
                insertValues[idx], value));
        }
        return insertValues;
    }

    private void addGeneratedPartitionedColumnValue(ColumnIdent columnIdent,
                                                    Object value,
                                                    Map<String, String> partitionMap) {
        assert partitionMap != null : "partitionMap must not be null";
        String generatedValue = BytesRefs.toString(value);
        String givenValue = partitionMap.get(columnIdent.name());
        if (givenValue == null) {
            partitionMap.put(columnIdent.name(), generatedValue);
        } else if (!givenValue.equals(generatedValue)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Given value %s for generated column does not match defined generated expression value %s",
                givenValue, generatedValue));
        }
    }

    static void verifyOnConflictTargets(Insert.DuplicateKeyContext duplicateKeyContext, DocTableInfo docTableInfo) {
        List<String> constraintColumns = duplicateKeyContext.getConstraintColumns();
        if (constraintColumns.isEmpty()) {
            return;
        }
        List<ColumnIdent> pkColumnIdents = docTableInfo.primaryKey();
        if (constraintColumns.size() != pkColumnIdents.size()) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH,
                    "Number of conflict targets (%s) did not match the number of primary key columns (%s)",
                    constraintColumns, pkColumnIdents));
        }
        Collection<Reference> constraintRefs = resolveTargetColumns(constraintColumns, docTableInfo, pkColumnIdents.size());
        for (Reference contraintRef : constraintRefs) {
            if (!pkColumnIdents.contains(contraintRef.column())) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH,
                        "Conflict target (%s) did not match the primary key columns (%s)",
                        constraintColumns, pkColumnIdents));
            }
        }
    }
}
