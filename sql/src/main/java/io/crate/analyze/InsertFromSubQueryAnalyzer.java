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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.InsertFromSubquery;

import java.util.*;

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
            assert reference != null;
            return new InputColumn(i, argumentColumn.valueType());
        }
    }

    InsertFromSubQueryAnalyzer(Functions functions, Schemas schemas, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.schemas = schemas;
        this.relationAnalyzer = relationAnalyzer;
    }


    public AnalyzedStatement analyze(InsertFromSubquery node, Analysis analysis) {
        DocTableInfo tableInfo = schemas.getWritableTable(
            TableIdent.of(node.table(), analysis.sessionContext().defaultSchema()));
        Operation.blockedRaiseException(tableInfo, Operation.INSERT);

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        FieldProvider fieldProvider = new NameFieldProvider(tableRelation);

        QueriedRelation source = (QueriedRelation) relationAnalyzer.analyze(node.subQuery(), analysis);

        List<Reference> targetColumns = new ArrayList<>(resolveTargetColumns(node.columns(), tableInfo, source.fields().size()));
        validateColumnsAndAddCastsIfNecessary(targetColumns, source.querySpec());

        Map<Reference, Symbol> onDuplicateKeyAssignments = null;
        if (!node.onDuplicateKeyAssignments().isEmpty()) {
            onDuplicateKeyAssignments = processUpdateAssignments(
                tableRelation,
                targetColumns,
                analysis.sessionContext(),
                analysis.parameterContext(),
                analysis.transactionContext(),
                fieldProvider,
                node.onDuplicateKeyAssignments());
        }

        return new InsertFromSubQueryAnalyzedStatement(
            source,
            tableInfo,
            targetColumns,
            onDuplicateKeyAssignments);
    }

    private static Collection<Reference> resolveTargetColumns(Collection<String> targetColumnNames,
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
                    throw new ColumnUnknownException(targetColumnName);
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
                                                              QuerySpec querySpec) {
        if (targetColumns.size() != querySpec.outputs().size()) {
            Joiner commaJoiner = Joiner.on(", ");
            throw new IllegalArgumentException(String.format("Number of target columns (%s) of insert statement doesn't match number of source columns (%s)",
                commaJoiner.join(Iterables.transform(targetColumns, Reference.TO_COLUMN_NAME)),
                commaJoiner.join(Iterables.transform(querySpec.outputs(), SymbolPrinter.FUNCTION))));
        }

        int failedCastPosition = querySpec.castOutputs(Iterators.transform(targetColumns.iterator(), Symbols.TYPES_FUNCTION));
        if (failedCastPosition >= 0) {
            Symbol failedSource = querySpec.outputs().get(failedCastPosition);
            Reference failedTarget = targetColumns.get(failedCastPosition);
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Type of subquery column %s (%s) does not match is not convertable to the type of table column %s (%s)",
                failedSource,
                failedSource.valueType(),
                failedTarget.ident().columnIdent().fqn(),
                failedTarget.valueType()
            ));
        }
    }

    private Map<Reference, Symbol> processUpdateAssignments(DocTableRelation tableRelation,
                                                            List<Reference> targetColumns,
                                                            SessionContext sessionContext,
                                                            ParameterContext parameterContext,
                                                            TransactionContext transactionContext,
                                                            FieldProvider fieldProvider,
                                                            List<Assignment> assignments) {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, sessionContext, parameterContext, fieldProvider, null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.CLUSTER,
            ReplaceMode.COPY,
            null,
            tableRelation);
        ValueNormalizer valuesNormalizer = new ValueNormalizer(schemas);

        ValuesResolver valuesResolver = new ValuesResolver(tableRelation, targetColumns);
        ValuesAwareExpressionAnalyzer valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
            functions, sessionContext, parameterContext, fieldProvider, valuesResolver);

        Map<Reference, Symbol> updateAssignments = new HashMap<>(assignments.size());
        for (Assignment assignment : assignments) {
            Reference columnName = tableRelation.resolveField(
                (Field) expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext));
            assert columnName != null;

            Symbol valueSymbol = normalizer.normalize(
                valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext),
                transactionContext);
            Symbol assignmentExpression = valuesNormalizer.normalizeInputForReference(valueSymbol, columnName);

            updateAssignments.put(columnName, assignmentExpression);
        }

        return updateAssignments;
    }
}
