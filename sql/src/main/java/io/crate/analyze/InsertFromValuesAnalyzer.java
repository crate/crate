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

import com.google.common.base.Function;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.operation.Input;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.ValuesList;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.*;

@Singleton
public class InsertFromValuesAnalyzer extends AbstractInsertAnalyzer {

    private static final ReferenceToLiteralConverter TO_LITERAL_CONVERTER = new ReferenceToLiteralConverter();

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
            assert columnReference != null;
            DataType returnType = columnReference.valueType();
            assignmentColumns.add(columnReference.ident().columnIdent().fqn());
            return Literal.newLiteral(returnType, returnType.value(insertValues[columns.indexOf(columnReference)]));
        }
    }

    @Inject
    public InsertFromValuesAnalyzer(AnalysisMetaData analysisMetaData) {
        super(analysisMetaData);
    }

    public AnalyzedStatement analyze(InsertFromValues node, Analysis analysis) {
        DocTableInfo tableInfo = analysisMetaData.schemas().getWritableTable(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        Operation.blockedRaiseException(tableInfo, Operation.INSERT);

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        FieldProvider fieldProvider = new NameFieldProvider(tableRelation);
        ExpressionAnalyzer expressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, analysis.parameterContext(), fieldProvider, tableRelation);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext(analysis.statementContext());
        expressionAnalyzer.setResolveFieldsOperation(Operation.INSERT);

        ValuesResolver valuesResolver = new ValuesResolver(tableRelation);
        ExpressionAnalyzer valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
                analysisMetaData, analysis.parameterContext(), fieldProvider, valuesResolver);

        InsertFromValuesAnalyzedStatement statement = new InsertFromValuesAnalyzedStatement(
                tableInfo, analysis.parameterContext().numBulkParams());
        handleInsertColumns(node, node.maxValuesLength(), statement);

        Set<ReferenceInfo> allReferencedReferences = new HashSet<>();
        for (GeneratedReferenceInfo generatedReferenceInfo : tableInfo.generatedColumns()) {
            allReferencedReferences.addAll(generatedReferenceInfo.referencedReferenceInfos());
        }
        ReferenceToLiteralConverter.Context referenceToLiteralContext = new ReferenceToLiteralConverter.Context(
                statement.columns(), allReferencedReferences);

        ValueNormalizer valuesNormalizer = new ValueNormalizer(analysisMetaData.schemas(),
            new EvaluatingNormalizer(
                analysisMetaData.functions(),
                RowGranularity.CLUSTER,
                analysisMetaData.referenceResolver(),
                tableRelation,
                false));
        for (ValuesList valuesList : node.valuesLists()) {
            analyzeValues(
                    tableRelation,
                    valuesNormalizer,
                    expressionAnalyzer,
                    expressionAnalysisContext,
                    valuesResolver,
                    valuesAwareExpressionAnalyzer,
                    valuesList,
                    node.onDuplicateKeyAssignments(),
                    statement,
                    analysis.parameterContext(),
                    referenceToLiteralContext);
        }
        return statement;
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
                        numInsertColumns-numAddedGeneratedColumns, numValues));
            }

        }
    }

    private void analyzeValues(DocTableRelation tableRelation,
                               ValueNormalizer valueNormalizer,
                               ExpressionAnalyzer expressionAnalyzer,
                               ExpressionAnalysisContext expressionAnalysisContext,
                               ValuesResolver valuesResolver,
                               ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                               ValuesList node,
                               List<Assignment> assignments,
                               InsertFromValuesAnalyzedStatement statement,
                               ParameterContext parameterContext,
                               ReferenceToLiteralConverter.Context referenceToLiteralContext) {
        validateValuesSize(node.values(), statement, tableRelation);

        try {
            DocTableInfo tableInfo = statement.tableInfo();
            int numPks = tableInfo.primaryKey().size();
            Function<List<BytesRef>, String> idFunction = Id.compile(tableInfo.primaryKey(), tableInfo.clusteredBy());
            if (parameterContext.numBulkParams() > 0) {
                for (int i = 0; i < parameterContext.numBulkParams(); i++) {
                    parameterContext.setBulkIdx(i);
                    addValues(
                        tableRelation,
                        valueNormalizer,
                        expressionAnalyzer,
                        expressionAnalysisContext,
                        valuesResolver,
                        valuesAwareExpressionAnalyzer,
                        node,
                        assignments,
                        statement,
                        referenceToLiteralContext,
                        numPks,
                        idFunction,
                        i
                    );
                }
            } else {
                addValues(
                    tableRelation,
                    valueNormalizer,
                    expressionAnalyzer,
                    expressionAnalysisContext,
                    valuesResolver,
                    valuesAwareExpressionAnalyzer,
                    node,
                    assignments,
                    statement,
                    referenceToLiteralContext,
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
                           ValueNormalizer valueNormalizer,
                           ExpressionAnalyzer expressionAnalyzer,
                           ExpressionAnalysisContext expressionAnalysisContext,
                           ValuesResolver valuesResolver,
                           ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                           ValuesList node,
                           List<Assignment> assignments,
                           InsertFromValuesAnalyzedStatement context,
                           ReferenceToLiteralConverter.Context referenceToLiteralContext,
                           int numPrimaryKeys,
                           Function<List<BytesRef>, String> idFunction,
                           int bulkIdx) throws IOException {
        if (context.tableInfo().isPartitioned()) {
            context.newPartitionMap();
        }
        List<BytesRef> primaryKeyValues = new ArrayList<>(numPrimaryKeys);
        String routingValue = null;
        List<ColumnIdent> primaryKey = context.tableInfo().primaryKey();
        Object[] insertValues = new Object[node.values().size()];

        for (int i = 0, valuesSize = node.values().size(); i < valuesSize; i++) {
            Expression expression = node.values().get(i);
            Symbol valuesSymbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);

            // implicit type conversion
            Reference column = context.columns().get(i);
            final ColumnIdent columnIdent = column.info().ident().columnIdent();
            Object value;
            try {
                valuesSymbol = valueNormalizer.normalizeInputForReference(
                    valuesSymbol, column, expressionAnalysisContext.statementContext());
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
                    assert value instanceof Map;
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

        if (!assignments.isEmpty()) {
            valuesResolver.insertValues = insertValues;
            valuesResolver.columns = context.columns();
            Symbol[] onDupKeyAssignments = new Symbol[assignments.size()];
            valuesResolver.assignmentColumns = new ArrayList<>(assignments.size());
            expressionAnalyzer.setResolveFieldsOperation(Operation.UPDATE);
            for (int i = 0; i < assignments.size(); i++) {
                Assignment assignment = assignments.get(i);
                Reference columnName = tableRelation.resolveField(
                        (Field) expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext));
                assert columnName != null;

                Symbol assignmentExpression = valueNormalizer.normalizeInputForReference(
                    valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext),
                    columnName,
                    expressionAnalysisContext.statementContext()
                );
                assignmentExpression = valuesAwareExpressionAnalyzer.normalize(
                    assignmentExpression, expressionAnalysisContext.statementContext());
                onDupKeyAssignments[i] = assignmentExpression;

                if (valuesResolver.assignmentColumns.size() == i) {
                    valuesResolver.assignmentColumns.add(columnName.ident().columnIdent().fqn());
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
            expressionAnalyzer,
            expressionAnalysisContext.statementContext(),
            referenceToLiteralContext,
            primaryKeyValues,
            insertValues,
            routingValue);
        processGeneratedExpressions(ctx);
        insertValues = ctx.insertValues;
        routingValue = ctx.routingValue;

        context.sourceMaps().add(insertValues);
        String id = idFunction.apply(primaryKeyValues);
        context.addIdAndRouting(id, routingValue);
        if (bulkIdx >= 0) {
            context.bulkIndices().add(bulkIdx);
        }
    }

    private void addPrimaryKeyValue(int index, Object value, List<BytesRef> primaryKeyValues) {
        if (value == null) {
            throw new IllegalArgumentException("Primary key value must not be NULL");
        }
        if (primaryKeyValues.size() > index) {
            primaryKeyValues.add(index, BytesRefs.toBytesRef(value));
        } else {
            primaryKeyValues.add(BytesRefs.toBytesRef(value));
        }
    }

    private String extractRoutingValue(ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        ColumnIdent clusteredByIdent = context.tableInfo().clusteredBy();
        assert clusteredByIdent != null : "clusteredByIdent must not be null";
        if (columnValue != null && !columnIdent.equals(clusteredByIdent)) {
            // oh my gosh! A nested clustered by value!!!
            assert columnValue instanceof Map;
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
            assert columnValue instanceof Map;
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
        }
        return null;
    }

    private static class GeneratedExpressionContext {

        private final DocTableRelation tableRelation;
        private final InsertFromValuesAnalyzedStatement analyzedStatement;
        private final ExpressionAnalyzer expressionAnalyzer;
        private final ReferenceToLiteralConverter.Context referenceToLiteralContext;
        private final StmtCtx stmtCtx;
        private final List<BytesRef> primaryKeyValues;

        private Object[] insertValues;
        private @Nullable String routingValue;

        private GeneratedExpressionContext(DocTableRelation tableRelation,
                                           InsertFromValuesAnalyzedStatement analyzedStatement,
                                           ExpressionAnalyzer expressionAnalyzer,
                                           StmtCtx stmtCtx,
                                           ReferenceToLiteralConverter.Context referenceToLiteralContext,
                                           List<BytesRef> primaryKeyValues,
                                           Object[] insertValues,
                                           @Nullable String routingValue) {
            this.tableRelation = tableRelation;
            this.analyzedStatement = analyzedStatement;
            this.expressionAnalyzer = expressionAnalyzer;
            this.stmtCtx = stmtCtx;
            this.primaryKeyValues = primaryKeyValues;
            this.insertValues = insertValues;
            this.routingValue = routingValue;
            this.referenceToLiteralContext = referenceToLiteralContext;
            referenceToLiteralContext.values(insertValues);
        }
    }

    private void processGeneratedExpressions(GeneratedExpressionContext context) {
        List<ColumnIdent> primaryKey = context.analyzedStatement.tableInfo().primaryKey();
        for (GeneratedReferenceInfo referenceInfo : context.tableRelation.tableInfo().generatedColumns()) {
            Reference reference = new Reference(referenceInfo);
            Symbol valueSymbol = TO_LITERAL_CONVERTER.process(referenceInfo.generatedExpression(), context.referenceToLiteralContext);
            valueSymbol = context.expressionAnalyzer.normalize(valueSymbol, context.stmtCtx);
            if (valueSymbol.symbolType() == SymbolType.LITERAL) {
                Object value = ((Input) valueSymbol).value();
                if (primaryKey.contains(referenceInfo.ident().columnIdent()) && context.analyzedStatement.columns().indexOf(reference) == -1) {
                    int idx = primaryKey.indexOf(referenceInfo.ident().columnIdent());
                    addPrimaryKeyValue(idx, value, context.primaryKeyValues);
                }
                ColumnIdent routingColumn = context.analyzedStatement.tableInfo().clusteredBy();
                if (routingColumn != null && routingColumn.equals(referenceInfo.ident().columnIdent())) {
                    context.routingValue = extractRoutingValue(routingColumn, value, context.analyzedStatement);
                }
                if (context.tableRelation.tableInfo().isPartitioned()
                    && context.tableRelation.tableInfo().partitionedByColumns().contains(referenceInfo)) {
                    addGeneratedPartitionedColumnValue(referenceInfo.ident().columnIdent(), value,
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
        assert partitionMap != null;
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

}
