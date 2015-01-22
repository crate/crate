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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

@Singleton
public class InsertFromValuesAnalyzer extends AbstractInsertAnalyzer {

    private ExpressionAnalyzer expressionAnalyzer;
    private ExpressionAnalysisContext expressionAnalysisContext;

    private ValuesAwareExpressionAnalyzer valuesAwareExpressionAnalyzer;

    private static class ValuesAwareExpressionAnalyzer extends ExpressionAnalyzer {

        private final ExpressionAnalyzer expressionAnalyzer;
        public Map<String, Object> insertValues;

        public ValuesAwareExpressionAnalyzer(AnalysisMetaData analysisMetaData,
                                             ParameterContext parameterContext,
                                             FieldProvider fieldProvider,
                                             ExpressionAnalyzer expressionAnalyzer) {
            super(analysisMetaData, parameterContext, fieldProvider);
            this.expressionAnalyzer = expressionAnalyzer;
        }

        @Override
        protected Symbol convertFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
            List<String> parts = node.getName().getParts();
            if (parts.get(0).equals("values")) {
                Expression expression = node.getArguments().get(0);
                Symbol argumentColumn = expressionAnalyzer.convert(expression, context);
                if (argumentColumn.valueType().equals(DataTypes.UNDEFINED)) {
                    throw new IllegalArgumentException(
                            SymbolFormatter.format("Referenced column '%s' in VALUES expression not found", argumentColumn));
                }
                if (!(argumentColumn instanceof Field)) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                            "Argument to VALUES expression must reference a column that " +
                                    "is part of the INSERT statement. %s is invalid", argumentColumn));
                }
                String outputName = ((ColumnIdent) ((Field) argumentColumn).path()).fqn();
                DataType returnType = argumentColumn.valueType();
                // use containsKey instead of checking result .get() for null because inserted value might actually be null
                if (!insertValues.containsKey(outputName)) {
                    throw new IllegalArgumentException(String.format(
                            "Referenced column '%s' isn't part of the column list of the INSERT statement", outputName));
                }
                return Literal.newLiteral(returnType, returnType.value(insertValues.get(outputName)));
            }
            return super.convertFunctionCall(node, context);
        }
    }

    @Inject
    protected InsertFromValuesAnalyzer(AnalysisMetaData analysisMetaData) {
        super(analysisMetaData);
    }

    @Override
    public AbstractInsertAnalyzedStatement visitInsertFromValues(InsertFromValues node, Analysis analysis) {
        if (!node.onDuplicateKeyAssignments().isEmpty()) {
            throw new UnsupportedOperationException("ON DUPLICATE KEY UPDATE clause is not supported");
        }
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node.table()));
        TableRelation tableRelation = new TableRelation(tableInfo);
        validateTable(tableInfo);

        FieldProvider fieldProvider = new NameFieldProvider(tableRelation);
        expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                analysis.parameterContext(),
                fieldProvider
                );
        expressionAnalyzer.resolveWritableFields(true);
        expressionAnalysisContext = new ExpressionAnalysisContext();

        valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
                analysisMetaData, analysis.parameterContext(), fieldProvider, expressionAnalyzer);

        InsertFromValuesAnalyzedStatement statement = new InsertFromValuesAnalyzedStatement(
                tableInfo, analysis.parameterContext().hasBulkParams());
        handleInsertColumns(node, node.maxValuesLength(), statement);

        for (ValuesList valuesList : node.valuesLists()) {
            analyzeValues(valuesList, node.onDuplicateKeyAssignments(), statement, analysis.parameterContext());
        }
        return statement;
    }

    private void validateTable(TableInfo tableInfo) throws UnsupportedOperationException, IllegalArgumentException {
        if (tableInfo.isAlias() && !tableInfo.isPartitioned()) {
            throw new UnsupportedOperationException("aliases are read only.");
        }
        if (tableInfo.schemaInfo().systemSchema()) {
            throw new UnsupportedOperationException("Can't insert into system tables, they are read only");
        }
    }

    private void analyzeValues(ValuesList node,  List<Assignment> assignments,
                               InsertFromValuesAnalyzedStatement statement,
                               ParameterContext parameterContext) {
        if (node.values().size() != statement.columns().size()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Invalid number of values: Got %d columns specified but %d values",
                    statement.columns().size(), node.values().size()));
        }
        try {
            int numPks = statement.tableInfo().primaryKey().size();
            if (parameterContext.bulkParameters.length > 0) {
                for (int i = 0; i < parameterContext.bulkParameters.length; i++) {
                    parameterContext.setBulkIdx(i);
                    addValues(node, assignments, statement, numPks);
                }
            } else {
                addValues(node, assignments, statement, numPks);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // can't throw IOException directly because of visitor interface
        }
    }

    private void addValues(ValuesList node,
                           List<Assignment> assignments,
                           InsertFromValuesAnalyzedStatement context,
                           int numPrimaryKeys) throws IOException {
        if (context.tableInfo().isPartitioned()) {
            context.newPartitionMap();
        }
        List<BytesRef> primaryKeyValues = new ArrayList<>(numPrimaryKeys);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        String routingValue = null;
        List<Expression> values = node.values();
        List<ColumnIdent> primaryKey = context.tableInfo().primaryKey();

        Map<String, Object> insertValues = new HashMap<>();

        for (int i = 0, valuesSize = values.size(); i < valuesSize; i++) {
            Expression expression = values.get(i);
            Symbol valuesSymbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);

            // implicit type conversion
            Reference column = context.columns().get(i);
            final ColumnIdent columnIdent = column.info().ident().columnIdent();
            try {
                valuesSymbol = expressionAnalyzer.normalizeInputForReference(valuesSymbol, column, true);
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
                throw new ColumnValidationException(column.info().ident().columnIdent().sqlFqn(), e);
            }

            try {
                Object value = ((Input) valuesSymbol).value();
                if (context.primaryKeyColumnIndices().contains(i)) {
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
                        builder.field(columnIdent.name(), rest);
                        if (!assignments.isEmpty()) {
                            insertValues.put(columnIdent.name(), rest);
                        }
                    }
                } else {
                    if (value instanceof BytesRef) {
                        value = new BytesText(new BytesArray((BytesRef) value));
                    }
                    builder.field(columnIdent.name(), value);
                    if (!assignments.isEmpty()) {
                        insertValues.put(columnIdent.name(), value);
                    }
                }
            } catch (ClassCastException e) {
                // symbol is no input
                throw new ColumnValidationException(columnIdent.name(),
                        String.format("Invalid value of type '%s' in insert statement", valuesSymbol.symbolType().name()));
            }
        }

        if (!assignments.isEmpty()) {
            Map<String, Symbol> onDupKeyAssignments = new HashMap<>();
            valuesAwareExpressionAnalyzer.insertValues = insertValues;
            for (Assignment assignment : assignments) {
                Symbol columnName = expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext);
                assert columnName instanceof Field : "columnName must be a field"; // ensured by parser

                Symbol assignmentExpression = valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext);
                assignmentExpression = valuesAwareExpressionAnalyzer.normalize(assignmentExpression);
                onDupKeyAssignments.put(((ColumnIdent) ((Field) columnName).path()).fqn(), assignmentExpression);
            }
            context.addOnDuplicateKeyAssignments(onDupKeyAssignments);
        }
        context.sourceMaps().add(builder.bytes());
        context.addIdAndRouting(primaryKeyValues, routingValue);
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
        Object clusteredByValue = columnValue;
        ColumnIdent clusteredByIdent = context.tableInfo().clusteredBy();
        if (!columnIdent.equals(clusteredByIdent)) {
            // oh my gosh! A nested clustered by value!!!
            assert clusteredByValue instanceof Map;
            clusteredByValue = StringObjectMaps.fromMapByPath((Map) clusteredByValue, clusteredByIdent.path());
        }
        if (clusteredByValue == null) {
            throw new IllegalArgumentException("Clustered by value must not be NULL");
        }
        return BytesRefs.toString(clusteredByValue);
    }

    private Object processPartitionedByValues(final ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        int idx = context.tableInfo().partitionedBy().indexOf(columnIdent);
        Map<String, String> partitionMap = context.currentPartitionMap();
        if (idx < 0) {
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
}
