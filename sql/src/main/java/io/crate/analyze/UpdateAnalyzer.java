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
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.*;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class UpdateAnalyzer {

    public static final String VERSION_SEARCH_EX_MSG =
        "_version is not allowed in update queries without specifying a primary key";

    private static final UnsupportedFeatureException VERSION_SEARCH_EX = new UnsupportedFeatureException(
        VERSION_SEARCH_EX_MSG);
    private static final Predicate<Reference> IS_OBJECT_ARRAY =
        input -> input != null
        && input.valueType().id() == ArrayType.ID
        && ((ArrayType) input.valueType()).innerType().equals(DataTypes.OBJECT);
    private static final UpdateSubscriptValidator UPDATE_SUBSCRIPT_VALIDATOR = new UpdateSubscriptValidator();

    private final Functions functions;
    private final RelationAnalyzer relationAnalyzer;
    private final ValueNormalizer valueNormalizer;


    UpdateAnalyzer(Functions functions, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.relationAnalyzer = relationAnalyzer;
        this.valueNormalizer = new ValueNormalizer();
    }

    public AnalyzedStatement analyze(Update node, Analysis analysis) {
        StatementAnalysisContext statementAnalysisContext = new StatementAnalysisContext(
            analysis.sessionContext(),
            analysis.parameterContext(),
            Operation.UPDATE,
            analysis.transactionContext());
        RelationAnalysisContext currentRelationContext = statementAnalysisContext.startRelation();
        AnalyzedRelation analyzedRelation = relationAnalyzer.analyze(node.relation(), statementAnalysisContext);

        FieldResolver fieldResolver = (FieldResolver) analyzedRelation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.CLUSTER,
            ReplaceMode.MUTATE,
            null,
            fieldResolver);
        FieldProvider columnFieldProvider = new NameFieldProvider(analyzedRelation);
        ExpressionAnalyzer columnExpressionAnalyzer = new ExpressionAnalyzer(
            functions,
            analysis.sessionContext(),
            analysis.parameterContext(),
            columnFieldProvider,
            null);
        columnExpressionAnalyzer.setResolveFieldsOperation(Operation.UPDATE);

        assert Iterables.getOnlyElement(currentRelationContext.sources().values()) == analyzedRelation :
            "currentRelationContext.sources().values() must have one element and equal to analyzedRelation";
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            analysis.sessionContext(),
            analysis.parameterContext(),
            new FullQualifedNameFieldProvider(currentRelationContext.sources()),
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
            WhereClause whereClause = new WhereClause(normalizer.normalize(querySymbol, analysis.transactionContext()));

            if (whereClauseAnalyzer != null) {
                whereClause = whereClauseAnalyzer.analyze(whereClause, analysis.transactionContext());
            }

            if (!whereClause.docKeys().isPresent() &&
                Symbols.containsColumn(whereClause.query(), DocSysColumns.VERSION)) {
                throw VERSION_SEARCH_EX;
            }

            UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement =
                new UpdateAnalyzedStatement.NestedAnalyzedStatement(whereClause);

            for (Assignment assignment : node.assignements()) {
                analyzeAssignment(
                    assignment,
                    nestedAnalyzedStatement,
                    tableInfo,
                    normalizer,
                    expressionAnalyzer,
                    columnExpressionAnalyzer,
                    expressionAnalysisContext,
                    analysis.transactionContext()
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
        UPDATE_SUBSCRIPT_VALIDATOR.process(node.columnName(), false);

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
            value = valueNormalizer.normalizeInputForReference(value, reference, tableInfo);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw new ColumnValidationException(ident.sqlFqn(), e);
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

    private static class UpdateSubscriptValidator extends AstVisitor<Void, Boolean> {

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, Boolean context) {
            process(node.index(), true);
            return super.visitSubscriptExpression(node, context);
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Boolean context) {
            if (context) {
                throw new IllegalArgumentException("Updating a single element of an array is not supported");
            }
            return null;
        }
    }
}
