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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
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
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.Update;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;

/**
 * Used to analyze statements like: `UPDATE t1 SET col1 = ? WHERE id = ?`
 */
public final class UpdateAnalyzer {

    private static final Predicate<Reference> IS_OBJECT_ARRAY =
        input -> input.valueType() instanceof ArrayType<?> arrayType && arrayType.innerType().id() == ObjectType.ID;

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
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(nodeCtx, RowGranularity.CLUSTER, null, table);
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
        for (int i = 0; i < assignments.size(); i++) {
            Assignment<Expression> assignment = assignments.get(i);
            AssignmentNameValidator.ensureNoArrayElementUpdate(assignment.columnName());

            Symbol target = normalizer.normalize(targetExprAnalyzer.convert(assignment.columnName(), exprCtx), txnCtx);
            assert target instanceof Reference : "AstBuilder restricts left side of assignments to Columns/Subscripts";
            Reference targetCol = (Reference) target;
            if (hasMatchingParent(tableInfo, targetCol, IS_OBJECT_ARRAY)) {
                // cannot update fields of object arrays
                throw new IllegalArgumentException("Updating fields of object arrays is not supported");
            }

            Symbol source = ValueNormalizer.normalizeInputForReference(
                normalizer.normalize(sourceExprAnalyzer.convert(assignment.expression(), exprCtx), txnCtx),
                targetCol,
                tableInfo,
                s -> normalizer.normalize(s, txnCtx)
            );
            if (assignmentByTargetCol.put(targetCol, source) != null) {
                throw new IllegalArgumentException("Target expression repeated: " + targetCol.column().sqlFqn());
            }
        }
        return assignmentByTargetCol;
    }

    private static boolean hasMatchingParent(TableInfo tableInfo, Reference info, Predicate<Reference> parentMatchPredicate) {
        for (var parent : tableInfo.getParents(info.column())) {
            if (parentMatchPredicate.test(parent)) {
                return true;
            }
        }
        return false;
    }

    private static class AssignmentNameValidator extends AstVisitor<Void, Boolean> {

        private static final AssignmentNameValidator INSTANCE = new AssignmentNameValidator();

        static void ensureNoArrayElementUpdate(Expression expression) {
            expression.accept(INSTANCE, false);
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, Boolean childOfSubscript) {
            node.index().accept(this, true);
            return super.visitSubscriptExpression(node, childOfSubscript);
        }

        private Void validateLiteral(Boolean childOfSubscript) {
            if (childOfSubscript) {
                throw new IllegalArgumentException("Updating a single element of an array is not supported");
            }
            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Boolean childOfSubscript) {
            return validateLiteral(childOfSubscript);
        }

        @Override
        protected Void visitIntegerLiteral(IntegerLiteral node, Boolean childOfSubscript) {
            return validateLiteral(childOfSubscript);
        }
    }
}
