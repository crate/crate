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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
import io.crate.common.collections.Lists;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
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
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

class InsertAnalyzer {

    private final NodeContext nodeCtx;
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

    InsertAnalyzer(NodeContext nodeCtx, Schemas schemas, RelationAnalyzer relationAnalyzer) {
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedInsertStatement analyze(Insert<Expression> insert, ParamTypeHints typeHints, CoordinatorTxnCtx txnCtx) {
        Set<String> uniqueColumns = new HashSet<>();
        for (String columnName: insert.columns()) {
            if (uniqueColumns.add(columnName) == false) {
                throw new IllegalArgumentException("column \"" + columnName + "\" specified more than once");
            }
        }
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            insert.table().getName(),
            Operation.INSERT,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath()
        );
        List<Reference> targetColumns;
        if (insert.columns().isEmpty()) {
            targetColumns = new ArrayList<>(tableInfo.columns());
        } else {
            targetColumns = insert.columns().stream()
                .map(col -> tableInfo.resolveColumn(col, true, true))
                .toList();
        }
        AnalyzedRelation subQueryRelation = relationAnalyzer.analyze(
            insert.insertSource(),
            new StatementAnalysisContext(typeHints, Operation.READ, txnCtx, targetColumns));

        ensureClusteredByPresentOrNotRequired(targetColumns, tableInfo);
        checkSourceAndTargetColsForLengthAndTypesCompatibility(targetColumns, subQueryRelation.outputs());

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        NameFieldProvider fieldProvider = new NameFieldProvider(tableRelation);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            typeHints,
            fieldProvider,
            null,
            Operation.READ
        );
        verifyOnConflictTargets(txnCtx, expressionAnalyzer, tableInfo, insert.duplicateKeyContext());
        Map<Reference, Symbol> onDuplicateKeyAssignments = processUpdateAssignments(
            tableRelation,
            targetColumns,
            typeHints,
            txnCtx,
            nodeCtx,
            fieldProvider,
            insert.duplicateKeyContext()
        );

        final boolean ignoreDuplicateKeys =
            insert.duplicateKeyContext().getType() == Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_NOTHING;

        List<Symbol> returnValues;
        if (insert.returningClause().isEmpty()) {
            returnValues = null;
        } else {
            var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
            Map<RelationName, AnalyzedRelation> sources = Map.of(tableRelation.relationName(), tableRelation);
            var sourceExprAnalyzer = new ExpressionAnalyzer(
                txnCtx,
                nodeCtx,
                typeHints,
                new FullQualifiedNameFieldProvider(
                    sources,
                    ParentRelations.NO_PARENTS,
                    txnCtx.sessionSettings().searchPath().currentSchema()),
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

    private static void verifyOnConflictTargets(CoordinatorTxnCtx txnCtx,
                                                ExpressionAnalyzer expressionAnalyzer,
                                                DocTableInfo docTable,
                                                Insert.DuplicateKeyContext<Expression> duplicateKeyContext) {
        List<Expression> constraintColumns = duplicateKeyContext.getConstraintColumns();
        if (constraintColumns.isEmpty()) {
            return;
        }
        List<ColumnIdent> pkColumnIdents = docTable.primaryKey();
        if (constraintColumns.size() != pkColumnIdents.size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "Number of conflict targets (%s) did not match the number of primary key columns (%s)",
                    constraintColumns, pkColumnIdents));
        }

        ExpressionAnalysisContext ctx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        List<Symbol> conflictTargets = Lists.map(constraintColumns, x -> {
            try {
                return expressionAnalyzer.convert(x, ctx);
            } catch (ColumnUnknownException e) {
                // Needed for BWC; to keep supporting `\"o.id\"` style subscript definition
                // Going through ExpressionAnalyzer again to still have a "column must exist" validation
                if (x instanceof QualifiedNameReference) {
                    QualifiedName name = ((QualifiedNameReference) x).getName();
                    Expression subscriptExpression = MetadataToASTNodeResolver.expressionFromColumn(
                        ColumnIdent.fromPath(name.toString())
                    );
                    return expressionAnalyzer.convert(subscriptExpression, ctx);
                }
                throw e;
            }
        });
        for (Symbol conflictTarget : conflictTargets) {
            if (!pkColumnIdents.contains(Symbols.pathFromSymbol(conflictTarget))) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ENGLISH,
                        "Conflict target (%s) did not match the primary key columns (%s)",
                        conflictTarget, pkColumnIdents));
            }
        }
    }

    private static void ensureClusteredByPresentOrNotRequired(List<Reference> targetColumnRefs, DocTableInfo tableInfo) {
        ColumnIdent clusteredBy = tableInfo.clusteredBy();
        if (clusteredBy == null || clusteredBy.equals(DocSysColumns.ID)) {
            return;
        }
        Reference clusteredByRef = tableInfo.getReference(clusteredBy);
        if (clusteredByRef.defaultExpression() != null) {
            return;
        }

        // target columns are always top level columns;
        // so we can compare against clusteredBy-root column in case clustered by is nested
        // In insert-from-query cases we cannot peek into object values to ensure the value is present
        // and need to rely on later runtime failures
        ColumnIdent clusteredByRoot = clusteredBy.getRoot();

        List<ColumnIdent> targetColumns = Lists.mapLazy(targetColumnRefs, Reference::column);
        if (targetColumns.contains(clusteredByRoot)) {
            return;
        }
        if (clusteredByRef instanceof GeneratedReference generatedClusteredBy) {
            var topLevelDependencies = Lists.mapLazy(generatedClusteredBy.referencedReferences(), x -> x.column().getRoot());
            if (targetColumns.containsAll(topLevelDependencies)) {
                return;
            }
        }

        throw new IllegalArgumentException(
            "Column `" + clusteredBy + "` is required but is missing from the insert statement");
    }

    private static void checkSourceAndTargetColsForLengthAndTypesCompatibility(List<Reference> targetColumns,
                                                                               List<Symbol> sources) {
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

    private static Map<Reference, Symbol> getUpdateAssignments(DocTableRelation targetTable,
                                                               List<Reference> targetCols,
                                                               ExpressionAnalyzer exprAnalyzer,
                                                               CoordinatorTxnCtx txnCtx,
                                                               NodeContext nodeCtx,
                                                               ParamTypeHints paramTypeHints,
                                                               Insert.DuplicateKeyContext<Expression> duplicateKeyContext) {
        if (duplicateKeyContext.getAssignments().isEmpty()) {
            return Collections.emptyMap();
        }

        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        ValuesResolver valuesResolver = new ValuesResolver(targetCols);
        final FieldProvider<?> fieldProvider;
        if (duplicateKeyContext.getType() == Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_UPDATE_SET) {
            fieldProvider = new ExcludedFieldProvider(new NameFieldProvider(targetTable), valuesResolver);
        } else {
            fieldProvider = new NameFieldProvider(targetTable);
        }
        var expressionAnalyzer = new ExpressionAnalyzer(txnCtx, nodeCtx, paramTypeHints, fieldProvider, null);
        var normalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.CLUSTER,
            null,
            targetTable,
            f -> f.signature().isDeterministic());
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
                                                            NodeContext nodeCtx,
                                                            FieldProvider<?> fieldProvider,
                                                            Insert.DuplicateKeyContext<Expression> duplicateKeyContext) {
        if (duplicateKeyContext.getAssignments().isEmpty()) {
            return Collections.emptyMap();
        }
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx, nodeCtx, paramTypeHints, fieldProvider, null, Operation.UPDATE);
        return getUpdateAssignments(
            tableRelation,
            targetColumns,
            expressionAnalyzer,
            coordinatorTxnCtx,
            nodeCtx,
            paramTypeHints,
            duplicateKeyContext
        );
    }
}
