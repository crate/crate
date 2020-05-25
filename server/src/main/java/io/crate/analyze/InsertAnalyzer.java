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
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.ExcludedFieldProvider;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Insert;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;

class InsertAnalyzer {

    private final Functions functions;
    private final Schemas schemas;
    private final RelationAnalyzer relationAnalyzer;

    private static class ValuesResolver implements io.crate.analyze.ValuesResolver {

        private final List<Reference> targetColumns;

        ValuesResolver(List<Reference> targetColumns) {
            this.targetColumns = targetColumns;
        }

        @Override
        public Symbol allocateAndResolve(Symbol argumentColumn) {
            int i = argumentColumn instanceof Reference
                ? targetColumns.indexOf(argumentColumn)
                : -1;
            if (i < 0) {
                throw new IllegalArgumentException(Symbols.format(
                    "Column '%s' that is used in the VALUES() expression is not part of the target column list",
                    argumentColumn));
            }
            return new InputColumn(i, argumentColumn.valueType());
        }
    }

    InsertAnalyzer(Functions functions, Schemas schemas, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.schemas = schemas;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedInsertStatement analyze(Insert<Expression> insert, ParamTypeHints typeHints, CoordinatorTxnCtx txnCtx) {
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            insert.table().getName(),
            Operation.INSERT,
            txnCtx.sessionContext().user(),
            txnCtx.sessionContext().searchPath()
        );
        List<Reference> targetColumns =
            new ArrayList<>(resolveTargetColumns(insert.columns(), tableInfo));

        AnalyzedRelation subQueryRelation = relationAnalyzer.analyze(
            insert.insertSource(),
            new StatementAnalysisContext(typeHints, Operation.READ, txnCtx, targetColumns));

        ensureClusteredByPresentOrNotRequired(targetColumns, tableInfo);
        checkSourceAndTargetColsForLengthAndTypesCompatibility(targetColumns, subQueryRelation.outputs());

        verifyOnConflictTargets(insert.duplicateKeyContext(), tableInfo);

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        Map<Reference, Symbol> onDuplicateKeyAssignments = processUpdateAssignments(
            tableRelation,
            targetColumns,
            typeHints,
            txnCtx,
            new NameFieldProvider(tableRelation),
            insert.duplicateKeyContext()
        );

        final boolean ignoreDuplicateKeys =
            insert.duplicateKeyContext().getType() == Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_NOTHING;

        List<Symbol> returnValues;
        if (insert.returningClause().isEmpty()) {
            returnValues = null;
        } else {
            var exprCtx = new ExpressionAnalysisContext();
            Map<RelationName, AnalyzedRelation> sources = Map.of(tableRelation.relationName(), tableRelation);
            var sourceExprAnalyzer = new ExpressionAnalyzer(
                functions,
                txnCtx,
                typeHints,
                new FullQualifiedNameFieldProvider(
                    sources,
                    ParentRelations.NO_PARENTS,
                    txnCtx.sessionContext().searchPath().currentSchema()
                ),
                null
            );
            var selectAnalysis = SelectAnalyzer.analyzeSelectItems(
                insert.returningClause(),
                sources,
                sourceExprAnalyzer,
                exprCtx
            );
            returnValues = selectAnalysis.outputSymbols();
        }
        return new AnalyzedInsertStatement(
            subQueryRelation,
            tableInfo,
            targetColumns,
            ignoreDuplicateKeys,
            onDuplicateKeyAssignments,
            returnValues
        );
    }

    private static void verifyOnConflictTargets(Insert.DuplicateKeyContext<?> duplicateKeyContext, DocTableInfo docTableInfo) {
        List<String> constraintColumns = duplicateKeyContext.getConstraintColumns();
        if (constraintColumns.isEmpty()) {
            return;
        }
        List<ColumnIdent> pkColumnIdents = docTableInfo.primaryKey();
        if (constraintColumns.size() != pkColumnIdents.size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "Number of conflict targets (%s) did not match the number of primary key columns (%s)",
                    constraintColumns, pkColumnIdents));
        }
        Collection<Reference> constraintRefs = resolveTargetColumns(constraintColumns, docTableInfo);
        for (Reference constraintRef : constraintRefs) {
            if (!pkColumnIdents.contains(constraintRef.column())) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ENGLISH,
                        "Conflict target (%s) did not match the primary key columns (%s)",
                        constraintColumns, pkColumnIdents));
            }
        }
    }

    private static Collection<Reference> resolveTargetColumns(Collection<String> targetColumnNames,
                                                              DocTableInfo targetTable) {
        if (targetColumnNames.isEmpty()) {
            return targetTable.columns();
        }
        LinkedHashSet<Reference> columns = new LinkedHashSet<>(targetColumnNames.size());
        for (String targetColumnName : targetColumnNames) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(targetColumnName);
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

    private static void ensureClusteredByPresentOrNotRequired(List<Reference> targetColumns, DocTableInfo tableInfo) {
        ColumnIdent clusteredBy = tableInfo.clusteredBy();
        if (clusteredBy != null &&
            !clusteredBy.name().equalsIgnoreCase(DocSysColumns.Names.ID) &&
            !targetColumns.contains(tableInfo.getReference(clusteredBy)) &&
            !tableInfo.primaryKey().contains(clusteredBy)) {

            var clusterByRef = tableInfo.getReference(clusteredBy);
            if (clusterByRef != null
                && clusterByRef.defaultExpression() == null
                && !isGeneratedColumnAndReferencedColumnsArePresent(clusteredBy, tableInfo)) {
                throw new IllegalArgumentException(
                    "Clustered by value is required but is missing from the insert statement");
            }
        }
    }

    private static boolean isGeneratedColumnAndReferencedColumnsArePresent(ColumnIdent columnIdent,
                                                                           DocTableInfo tableInfo) {
        Reference reference = tableInfo.getReference(columnIdent);
        if (reference instanceof GeneratedReference) {
            for (Reference referencedReference : ((GeneratedReference) reference).referencedReferences()) {
                for (Reference columnRef : tableInfo.columns()) {
                    if (columnRef.equals(referencedReference) ||
                        referencedReference.column().isChildOf(columnRef.column())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static void checkSourceAndTargetColsForLengthAndTypesCompatibility(
        List<Reference> targetColumns, List<Symbol> sources) {
        if (targetColumns.size() != sources.size()) {
            Collector<CharSequence, ?, String> commaJoiner = Collectors.joining(", ");
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Number of target columns (%s) of insert statement doesn't match number of source columns (%s)",
                targetColumns.stream().map(r -> r.column().sqlFqn()).collect(commaJoiner),
                sources.stream().map(Symbol::toString).collect(commaJoiner)));
        }

        for (int i = 0; i < targetColumns.size(); i++) {
            Reference targetCol = targetColumns.get(i);
            Symbol source = sources.get(i);
            DataType<?> targetType = targetCol.valueType();
            if (targetType.id() == DataTypes.UNDEFINED.id() || source.valueType().isConvertableTo(targetType, false)) {
                continue;
            }
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "The type '%s' of the insert source '%s' is not convertible to the type '%s' of target column '%s'",
                source.valueType(),
                source,
                targetType,
                targetCol.column().fqn()
            ));
        }
    }

    private static Map<Reference, Symbol> getUpdateAssignments(Functions functions,
                                                               DocTableRelation targetTable,
                                                               List<Reference> targetCols,
                                                               ExpressionAnalyzer exprAnalyzer,
                                                               CoordinatorTxnCtx txnCtx,
                                                               ParamTypeHints paramTypeHints,
                                                               Insert.DuplicateKeyContext<Expression> duplicateKeyContext) {
        if (duplicateKeyContext.getAssignments().isEmpty()) {
            return Collections.emptyMap();
        }

        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext();
        ValuesResolver valuesResolver = new ValuesResolver(targetCols);
        final FieldProvider<?> fieldProvider;
        if (duplicateKeyContext.getType() == Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_UPDATE_SET) {
            fieldProvider = new ExcludedFieldProvider(new NameFieldProvider(targetTable), valuesResolver);
        } else {
            fieldProvider = new NameFieldProvider(targetTable);
        }
        var expressionAnalyzer = new ExpressionAnalyzer(functions, txnCtx, paramTypeHints, fieldProvider, null);
        var normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, targetTable);
        Map<Reference, Symbol> updateAssignments = new HashMap<>(duplicateKeyContext.getAssignments().size());
        for (Assignment<Expression> assignment : duplicateKeyContext.getAssignments()) {
            Reference targetCol = (Reference) exprAnalyzer.convert(assignment.columnName(), exprCtx);
            Symbol valueSymbol = ValueNormalizer.normalizeInputForReference(
                normalizer.normalize(expressionAnalyzer.convert(assignment.expression(), exprCtx), txnCtx),
                targetCol,
                targetTable.tableInfo(),
                s -> normalizer.normalize(s, txnCtx)
            );
            updateAssignments.put(targetCol, valueSymbol);
        }
        return updateAssignments;
    }

    private Map<Reference, Symbol> processUpdateAssignments(DocTableRelation tableRelation,
                                                            List<Reference> targetColumns,
                                                            ParamTypeHints paramTypeHints,
                                                            CoordinatorTxnCtx coordinatorTxnCtx,
                                                            FieldProvider<?> fieldProvider,
                                                            Insert.DuplicateKeyContext<Expression> duplicateKeyContext) {
        if (duplicateKeyContext.getAssignments().isEmpty()) {
            return Collections.emptyMap();
        }
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, coordinatorTxnCtx, paramTypeHints, fieldProvider, null, Operation.UPDATE);
        return getUpdateAssignments(functions, tableRelation, targetColumns, expressionAnalyzer,
            coordinatorTxnCtx, paramTypeHints, duplicateKeyContext);
    }
}
