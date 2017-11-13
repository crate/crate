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

import com.google.common.collect.Iterables;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.Update;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.RandomAccess;
import java.util.function.Predicate;

/**
 * Used to analyze statements like: `UPDATE t1 SET col1 = ? WHERE id = ?`
 */
public class UpdateAnalyzer {

    public static final String VERSION_SEARCH_EX_MSG =
        "_version is not allowed in update queries without specifying a primary key";

    private static final UnsupportedFeatureException VERSION_SEARCH_EX = new UnsupportedFeatureException(
        VERSION_SEARCH_EX_MSG);
    private static final Predicate<Reference> IS_OBJECT_ARRAY =
        input -> input != null
        && input.valueType().id() == ArrayType.ID
        && ((ArrayType) input.valueType()).innerType().equals(DataTypes.OBJECT);

    private final Functions functions;
    private final RelationAnalyzer relationAnalyzer;


    UpdateAnalyzer(Functions functions, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedUpdateStatement analyze(Update update, ParamTypeHints typeHints, TransactionContext txnCtx) {
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
        if (!(relation instanceof AbstractTableRelation)) {
            throw new UnsupportedOperationException("UPDATE is only supported on base-tables");
        }
        AbstractTableRelation table = (AbstractTableRelation) relation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, table);
        ExpressionAnalyzer sourceExprAnalyzer = new ExpressionAnalyzer(
            functions,
            txnCtx,
            typeHints,
            new FullQualifiedNameFieldProvider(
                relCtx.sources(),
                relCtx.parentSources(),
                txnCtx.sessionContext().defaultSchema()
            ),
            null
        );
        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext();

        HashMap<Reference, Symbol> assignmentByTargetCol = getAssignments(
            update.assignements(), typeHints, txnCtx, table, normalizer, sourceExprAnalyzer, exprCtx);
        return new AnalyzedUpdateStatement(
            table,
            assignmentByTargetCol,
            normalizer.normalize(sourceExprAnalyzer.generateQuerySymbol(update.whereClause(), exprCtx), txnCtx)
        );
    }

    private HashMap<Reference, Symbol> getAssignments(List<Assignment> assignments,
                                                      ParamTypeHints typeHints,
                                                      TransactionContext txnCtx,
                                                      AbstractTableRelation table,
                                                      EvaluatingNormalizer normalizer,
                                                      ExpressionAnalyzer sourceExprAnalyzer,
                                                      ExpressionAnalysisContext exprCtx) {
        HashMap<Reference, Symbol> assignmentByTargetCol = new HashMap<>();
        ExpressionAnalyzer targetExprAnalyzer = new ExpressionAnalyzer(
            functions,
            txnCtx,
            typeHints,
            new NameFieldProvider(table),
            null
        );
        targetExprAnalyzer.setResolveFieldsOperation(Operation.UPDATE);
        assert assignments instanceof RandomAccess
            : "assignments should implement RandomAccess for indexed loop to avoid iterator allocations";
        for (int i = 0; i < assignments.size(); i++) {
            Assignment assignment = assignments.get(i);
            AssignmentNameValidator.ensureNoArrayElementUpdate(assignment.columnName());

            Symbol target = normalizer.normalize(targetExprAnalyzer.convert(assignment.columnName(), exprCtx), txnCtx);
            assert target instanceof Reference : "AstBuilder restricts left side of assignments to Columns/Subscripts";
            Reference targetCol = (Reference) target;

            Symbol source = normalizer.normalize(sourceExprAnalyzer.convert(assignment.expression(), exprCtx), txnCtx);
            try {
                source = ValueNormalizer.normalizeInputForReference(source, targetCol, table.tableInfo());
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
                throw new ColumnValidationException(targetCol.ident().columnIdent().sqlFqn(), table.tableInfo().ident(), e);
            }

            if (assignmentByTargetCol.put(targetCol, source) != null) {
                throw new IllegalArgumentException("Target expression repeated: " + targetCol.ident().columnIdent().sqlFqn());
            }
        }
        return assignmentByTargetCol;
    }

    /**
     * @deprecated This analyze variant uses the parameters and is bulk aware.
     *             Use {@link #analyze(Update, ParamTypeHints, TransactionContext)} instead
     */
    @Deprecated
    public AnalyzedStatement analyze(Update node, Analysis analysis) {
        final TransactionContext transactionContext = analysis.transactionContext();
        StatementAnalysisContext statementAnalysisContext = new StatementAnalysisContext(
            analysis.parameterContext(),
            Operation.UPDATE,
            transactionContext);
        RelationAnalysisContext currentRelationContext = statementAnalysisContext.startRelation();
        AnalyzedRelation analyzedRelation = relationAnalyzer.analyze(node.relation(), statementAnalysisContext);

        FieldResolver fieldResolver = (FieldResolver) analyzedRelation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.CLUSTER,
            null,
            fieldResolver);
        FieldProvider columnFieldProvider = new NameFieldProvider(analyzedRelation);
        ExpressionAnalyzer columnExpressionAnalyzer = new ExpressionAnalyzer(
            functions,
            transactionContext,
            analysis.parameterContext(),
            columnFieldProvider,
            null);
        columnExpressionAnalyzer.setResolveFieldsOperation(Operation.UPDATE);

        assert Iterables.getOnlyElement(currentRelationContext.sources().values()) == analyzedRelation :
            "currentRelationContext.sources().values() must have one element and equal to analyzedRelation";
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            transactionContext,
            analysis.parameterContext(),
            new FullQualifiedNameFieldProvider(
                currentRelationContext.sources(),
                currentRelationContext.parentSources(),
                transactionContext.sessionContext().defaultSchema()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        int numNested = 1;
        if (analysis.parameterContext().numBulkParams() > 0) {
            numNested = analysis.parameterContext().numBulkParams();
        }

        WhereClauseAnalyzer whereClauseAnalyzer = null;
        if (analyzedRelation instanceof DocTableRelation) {
            whereClauseAnalyzer = new WhereClauseAnalyzer(functions, ((DocTableRelation) analyzedRelation));
        }
        TableInfo tableInfo = ((AbstractTableRelation) analyzedRelation).tableInfo();

        List<UpdateAnalyzedStatement.NestedAnalyzedStatement> nestedAnalyzedStatements = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            analysis.parameterContext().setBulkIdx(i);

            Symbol querySymbol = expressionAnalyzer.generateQuerySymbol(node.whereClause(), expressionAnalysisContext);
            querySymbol = normalizer.normalize(querySymbol, transactionContext);

            WhereClause where;
            if (whereClauseAnalyzer != null) {
                where = whereClauseAnalyzer.analyze(querySymbol, transactionContext);
            } else {
                where = new WhereClause(querySymbol);
            }

            if (!where.docKeys().isPresent() && Symbols.containsColumn(where.query(), DocSysColumns.VERSION)) {
                throw VERSION_SEARCH_EX;
            }

            UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement =
                new UpdateAnalyzedStatement.NestedAnalyzedStatement(where);

            for (Assignment assignment : node.assignements()) {
                analyzeAssignment(
                    assignment,
                    nestedAnalyzedStatement,
                    tableInfo,
                    normalizer,
                    expressionAnalyzer,
                    columnExpressionAnalyzer,
                    expressionAnalysisContext,
                    transactionContext
                );
            }
            nestedAnalyzedStatements.add(nestedAnalyzedStatement);
        }

        statementAnalysisContext.endRelation();
        return new UpdateAnalyzedStatement(analyzedRelation, nestedAnalyzedStatements);
    }

    private void analyzeAssignment(Assignment node,
                                   UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement,
                                   TableInfo tableInfo,
                                   EvaluatingNormalizer normalizer,
                                   ExpressionAnalyzer expressionAnalyzer,
                                   ExpressionAnalyzer columnExpressionAnalyzer,
                                   ExpressionAnalysisContext expressionAnalysisContext,
                                   TransactionContext transactionContext) {
        AssignmentNameValidator.ensureNoArrayElementUpdate(node.columnName());

        // unknown columns in strict objects handled in here
        Reference reference = (Reference) normalizer.normalize(
            columnExpressionAnalyzer.convert(node.columnName(), expressionAnalysisContext),
            transactionContext);

        final ColumnIdent ident = reference.ident().columnIdent();
        if (hasMatchingParent(tableInfo, reference, IS_OBJECT_ARRAY)) {
            // cannot update fields of object arrays
            throw new IllegalArgumentException("Updating fields of object arrays is not supported");
        }
        Symbol value = normalizer.normalize(
            expressionAnalyzer.convert(node.expression(), expressionAnalysisContext), transactionContext);
        try {
            value = ValueNormalizer.normalizeInputForReference(value, reference, tableInfo);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw new ColumnValidationException(ident.sqlFqn(), tableInfo.ident(), e);
        }

        nestedAnalyzedStatement.addAssignment(reference, value);
    }


    private boolean hasMatchingParent(TableInfo tableInfo, Reference info, Predicate<Reference> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            Reference parentInfo = tableInfo.getReference(parent);
            if (parentMatchPredicate.test(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    private static class AssignmentNameValidator extends AstVisitor<Void, Boolean> {

        private static final AssignmentNameValidator INSTANCE = new AssignmentNameValidator();

        static void ensureNoArrayElementUpdate(Expression expression) {
            INSTANCE.process(expression, false);
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, Boolean childOfSubscript) {
            process(node.index(), true);
            return super.visitSubscriptExpression(node, childOfSubscript);
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Boolean childOfSubscript) {
            if (childOfSubscript) {
                throw new IllegalArgumentException("Updating a single element of an array is not supported");
            }
            return null;
        }
    }
}
