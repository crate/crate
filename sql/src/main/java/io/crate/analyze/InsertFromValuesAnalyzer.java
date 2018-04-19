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
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.data.Input;
import io.crate.exceptions.ColumnValidationException;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceToLiteralConverter;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
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

import static io.crate.analyze.InsertFromSubQueryAnalyzer.getUpdateAssignments;
import static io.crate.analyze.InsertFromSubQueryAnalyzer.resolveTargetColumns;

class InsertFromValuesAnalyzer extends AbstractInsertAnalyzer {

    private static class ValuesResolver implements io.crate.analyze.ValuesAwareExpressionAnalyzer.ValuesResolver {

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
                    argumentColumn.path().outputName()));
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


    public AnalyzedInsertStatement analyze(InsertFromValues insert, ParamTypeHints typeHints, TransactionContext txnCtx) {
        if (insert.valuesLists().isEmpty()) {
            throw new IllegalArgumentException("VALUES clause must not be empty");
        }

        RelationName relationName = RelationName.of(insert.table(), txnCtx.sessionContext().defaultSchema());
        DocTableInfo targetTable = schemas.getTableInfo(relationName, Operation.INSERT);
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

    public AnalyzedStatement analyze(InsertFromValues node, Analysis analysis) {
        DocTableInfo tableInfo = schemas.getTableInfo(
            RelationName.of(node.table(), analysis.sessionContext().defaultSchema()),
            Operation.INSERT
        );

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

        ExpressionAnalyzer valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
            functions,
            analysis.transactionContext(),
            convertParamFunction,
            fieldProvider,
            valuesResolver,
            duplicateKeyContext.getType());

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
        int numValues = values.size();
        int numInsertColumns = statement.columns().size();
        int numAddedGeneratedColumns = statement.numAddedGeneratedColumns();

        boolean firstValues = statement.sourceMaps().isEmpty();

        if (numValues != numInsertColumns) {
            if (firstValues
                || tableRelation.tableInfo().generatedColumns().isEmpty()
                || numValues != numInsertColumns - numAddedGeneratedColumns) {
                // do not fail here when numbers are different if:
                //  * we are not at the first values AND
                //  * we have generated columns in the table AND
                //  * we are only missing the generated columns which will be added
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Invalid number of values: Got %d columns specified but %d values",
                    numInsertColumns - numAddedGeneratedColumns, numValues));
            }

        }
    }

    private void analyzeValues(DocTableRelation tableRelation,
                               EvaluatingNormalizer normalizer,
                               ExpressionAnalyzer expressionAnalyzer,
                               ExpressionAnalysisContext expressionAnalysisContext,
                               TransactionContext transactionContext,
                               ValuesResolver valuesResolver,
                               ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                               ValuesList node,
                               List<Assignment> onDuplicateKeyAssignments,
                               InsertFromValuesAnalyzedStatement statement,
                               ParameterContext parameterContext,
                               ReferenceToLiteralConverter refToLiteral) {
        validateValuesSize(node.values(), statement, tableRelation);

        try {
            DocTableInfo tableInfo = statement.tableInfo();
            int numPks = tableInfo.primaryKey().size();
            Function<List<BytesRef>, String> idFunction =
                Id.compileWithNullValidation(tableInfo.primaryKey(), tableInfo.clusteredBy());
            if (parameterContext.numBulkParams() > 0) {
                for (int i = 0; i < parameterContext.numBulkParams(); i++) {
                    parameterContext.setBulkIdx(i);
                    addValues(
                        tableRelation,
                        normalizer,
                        expressionAnalyzer,
                        expressionAnalysisContext,
                        transactionContext,
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
                    transactionContext,
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
                           TransactionContext transactionContext,
                           ValuesResolver valuesResolver,
                           ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                           ValuesList node,
                           List<Assignment> onDuplicateKeyAssignments,
                           InsertFromValuesAnalyzedStatement context,
                           ReferenceToLiteralConverter refToLiteral,
                           int numPrimaryKeys,
                           Function<List<BytesRef>, String> idFunction,
                           int bulkIdx) throws IOException {
        DocTableInfo tableInfo = context.tableInfo();
        if (tableInfo.isPartitioned()) {
            context.newPartitionMap();
        }
        BytesRef[] primaryKeyValues = new BytesRef[numPrimaryKeys];
        String routingValue = null;
        List<ColumnIdent> primaryKey = tableInfo.primaryKey();
        Object[] insertValues = new Object[node.values().size()];

        for (int i = 0, valuesSize = node.values().size(); i < valuesSize; i++) {
            Reference column = context.columns().get(i);
            final ColumnIdent columnIdent = column.column();
            Expression expression = node.values().get(i);
            Symbol valuesSymbol = normalizer.normalize(
                expressionAnalyzer.convert(expression, expressionAnalysisContext),
                transactionContext);

            // implicit type conversion
            Object value;
            try {
                valuesSymbol = ValueNormalizer.normalizeInputForReference(valuesSymbol, column, tableRelation.tableInfo());
                value = ((Input) valuesSymbol).value();
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
                throw new ColumnValidationException(columnIdent.sqlFqn(), e);
            } catch (ClassCastException e) {
                // symbol is no Input
                throw new ColumnValidationException(columnIdent.name(),
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
                        Object nestedValue = StringObjectMaps.fromMapByPath((Map) value, pkIdent.path());
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
                Assignment assignment = onDuplicateKeyAssignments.get(i);
                Reference columnName = tableRelation.resolveField(
                    (Field) expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext));
                assert columnName != null : "columnName must not be null";

                Symbol valueSymbol = normalizer.normalize(
                    valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext),
                    transactionContext);
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
        GeneratedExpressionContext ctx = new GeneratedExpressionContext(
            tableRelation,
            context,
            normalizer,
            transactionContext,
            refToLiteral,
            primaryKeyValues,
            insertValues,
            routingValue);
        processGeneratedExpressions(ctx);
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
    private void addPrimaryKeyValue(int index, Object value, BytesRef[] primaryKeyValues) {
        if (value == null) {
            throw new IllegalArgumentException("Primary key value must not be NULL");
        }
        assert primaryKeyValues.length > index : "Index of primary key value is greater than the array holding the values";
        primaryKeyValues[index] = BytesRefs.toBytesRef(value);
    }

    private String extractRoutingValue(ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        ColumnIdent clusteredByIdent = context.tableInfo().clusteredBy();
        assert clusteredByIdent != null : "clusteredByIdent must not be null";
        if (columnValue != null && !columnIdent.equals(clusteredByIdent)) {
            // oh my gosh! A nested clustered by value!!!
            assert columnValue instanceof Map : "columnValue must be instance of Map";
            columnValue = StringObjectMaps.fromMapByPath((Map) columnValue, clusteredByIdent.path());
        }
        if (columnValue == null) {
            throw new IllegalArgumentException("Clustered by value must not be NULL");
        }
        return BytesRefs.toString(columnValue);
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
                    Object nestedValue = mapValue.remove(StringUtils.PATH_JOINER.join(partitionIdent.path()));
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

    private static class GeneratedExpressionContext {

        private final DocTableRelation tableRelation;
        private final InsertFromValuesAnalyzedStatement analyzedStatement;
        private final ReferenceToLiteralConverter refToLiteral;
        private final TransactionContext transactionContext;
        private final BytesRef[] primaryKeyValues;
        private final EvaluatingNormalizer normalizer;

        private Object[] insertValues;
        @Nullable
        private String routingValue;

        private GeneratedExpressionContext(DocTableRelation tableRelation,
                                           InsertFromValuesAnalyzedStatement analyzedStatement,
                                           EvaluatingNormalizer normalizer,
                                           TransactionContext transactionContext,
                                           ReferenceToLiteralConverter refToLiteral,
                                           BytesRef[] primaryKeyValues,
                                           Object[] insertValues,
                                           @Nullable String routingValue) {
            this.tableRelation = tableRelation;
            this.analyzedStatement = analyzedStatement;
            this.transactionContext = transactionContext;
            this.primaryKeyValues = primaryKeyValues;
            this.insertValues = insertValues;
            this.routingValue = routingValue;
            this.refToLiteral = refToLiteral;
            this.normalizer = normalizer;
            refToLiteral.values(insertValues);
        }
    }

    private void processGeneratedExpressions(GeneratedExpressionContext context) {
        List<ColumnIdent> primaryKey = context.analyzedStatement.tableInfo().primaryKey();
        for (GeneratedReference reference : context.tableRelation.tableInfo().generatedColumns()) {
            Symbol valueSymbol = RefReplacer.replaceRefs(reference.generatedExpression(), context.refToLiteral);
            valueSymbol = context.normalizer.normalize(valueSymbol, context.transactionContext);
            if (valueSymbol.symbolType() == SymbolType.LITERAL) {
                Object value = ((Input) valueSymbol).value();
                if (primaryKey.contains(reference.column())) {
                    int idx = primaryKey.indexOf(reference.column());
                    addPrimaryKeyValue(idx, value, context.primaryKeyValues);
                }
                ColumnIdent routingColumn = context.analyzedStatement.tableInfo().clusteredBy();
                if (routingColumn != null && routingColumn.equals(reference.column())) {
                    context.routingValue = extractRoutingValue(routingColumn, value, context.analyzedStatement);
                }
                if (context.tableRelation.tableInfo().isPartitioned()
                    && context.tableRelation.tableInfo().partitionedByColumns().contains(reference)) {
                    addGeneratedPartitionedColumnValue(reference.column(), value,
                        context.analyzedStatement.currentPartitionMap());
                } else {
                    context.insertValues = addGeneratedColumnValue(context.analyzedStatement, reference, value, context.insertValues);
                }
            }
        }
    }

    private Object[] addGeneratedColumnValue(InsertFromValuesAnalyzedStatement context,
                                             Reference reference,
                                             Object value,
                                             Object[] insertValues) {
        int idx = context.columns().indexOf(reference);
        if (idx == -1) {
            // add column & value
            context.addGeneratedColumn(reference);
            int valuesIdx = insertValues.length;
            insertValues = Arrays.copyOf(insertValues, insertValues.length + 1);
            insertValues[valuesIdx] = value;
        } else if (insertValues.length <= idx) {
            // only add value
            insertValues = Arrays.copyOf(insertValues, idx + 1);
            insertValues[idx] = value;
        } else if ((insertValues[idx] == null && value != null) ||
                   (insertValues[idx] != null && !insertValues[idx].equals(value))) {
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
