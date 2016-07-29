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
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.InsertFromSubquery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.*;

@Singleton
public class InsertFromSubQueryAnalyzer {

    private final AnalysisMetaData analysisMetaData;
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

    @Inject
    public InsertFromSubQueryAnalyzer(AnalysisMetaData analysisMetaData, RelationAnalyzer relationAnalyzer) {
        this.analysisMetaData = analysisMetaData;
        this.relationAnalyzer = relationAnalyzer;
    }


    public AnalyzedStatement analyze(InsertFromSubquery node, Analysis analysis) {
        DocTableInfo tableInfo = analysisMetaData.schemas().getWritableTable(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        Operation.blockedRaiseException(tableInfo, Operation.INSERT);

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        FieldProvider fieldProvider = new NameFieldProvider(tableRelation);

        QueriedRelation source = (QueriedRelation) relationAnalyzer.analyze(node.subQuery(), analysis);

        // We forbid using limit/offset or order by until we've implemented ES paging support (aka 'scroll')
        // TODO: move this to the consumer
        if (source.querySpec().isLimited() || source.querySpec().orderBy().isPresent()) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not " +
                    "supported on insert using a sub-query");
        }

        List<Reference> targetColumns = new ArrayList<>(resolveTargetColumns(node.columns(), tableInfo, source.fields().size()));
        validateColumnsAndAddCastsIfNecessary(targetColumns, source.querySpec());

        Map<Reference, Symbol> onDuplicateKeyAssignments = null;
        if (!node.onDuplicateKeyAssignments().isEmpty()) {
            onDuplicateKeyAssignments = processUpdateAssignments(
                    tableRelation,
                    targetColumns,
                    analysis.parameterContext(),
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
            ReferenceInfo referenceInfo = targetTable.getReferenceInfo(columnIdent);
            Reference targetReference;
            if (referenceInfo == null) {
                DynamicReference reference = targetTable.getDynamic(columnIdent, true);
                if (reference == null) {
                    throw new ColumnUnknownException(targetColumnName);
                }
                targetReference = reference;
            } else {
                targetReference = new Reference(referenceInfo);
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
        for (ReferenceInfo referenceInfo : targetTable.columns()) {
            if (idx > numSourceColumns) {
                break;
            }
            columns.add(new Reference(referenceInfo));
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
                    failedTarget.info().ident().columnIdent().fqn(),
                    failedTarget.valueType()
            ));
        }
    }

    private Map<Reference, Symbol> processUpdateAssignments(DocTableRelation tableRelation,
                                                            List<Reference> targetColumns,
                                                            ParameterContext parameterContext,
                                                            FieldProvider fieldProvider,
                                                            List<Assignment> assignments) {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData, parameterContext, fieldProvider, tableRelation);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        ValueNormalizer valuesNormalizer = new ValueNormalizer(analysisMetaData.schemas(), new EvaluatingNormalizer(
                analysisMetaData.functions(), RowGranularity.CLUSTER, analysisMetaData.referenceResolver(), tableRelation, false));

        ValuesResolver valuesResolver = new ValuesResolver(tableRelation, targetColumns);
        ValuesAwareExpressionAnalyzer valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
                analysisMetaData, parameterContext, fieldProvider, valuesResolver);

        Map<Reference, Symbol> updateAssignments = new HashMap<>(assignments.size());
        for (Assignment assignment : assignments) {
            Reference columnName = tableRelation.resolveField(
                    (Field) expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext));
            assert columnName != null;

            Symbol assignmentExpression = valuesNormalizer.normalizeInputForReference(
                    valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext),
                    columnName);

            updateAssignments.put(columnName, assignmentExpression);
        }

        return updateAssignments;
    }
}
