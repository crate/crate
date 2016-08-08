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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Update;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


@Singleton
public class UpdateStatementAnalyzer extends DefaultTraversalVisitor<AnalyzedStatement, Analysis> {

    public static final String VERSION_SEARCH_EX_MSG =
            "_version is not allowed in update queries without specifying a primary key";
    private static final UnsupportedFeatureException VERSION_SEARCH_EX = new UnsupportedFeatureException(
            VERSION_SEARCH_EX_MSG);


    private static final Predicate<ReferenceInfo> IS_OBJECT_ARRAY = new Predicate<ReferenceInfo>() {
        @Override
        public boolean apply(@Nullable ReferenceInfo input) {
            return input != null
                    && input.type().id() == ArrayType.ID
                    && ((ArrayType)input.type()).innerType().equals(DataTypes.OBJECT);
        }
    };


    private final AnalysisMetaData analysisMetaData;
    private final RelationAnalyzer relationAnalyzer;
    private final ValueNormalizer valueNormalizer;


    @Inject
    public UpdateStatementAnalyzer(AnalysisMetaData analysisMetaData,
                                   RelationAnalyzer relationAnalyzer) {
        this.analysisMetaData = analysisMetaData;
        this.relationAnalyzer = relationAnalyzer;
        this.valueNormalizer = new ValueNormalizer(analysisMetaData.schemas(), new EvaluatingNormalizer(
                analysisMetaData.functions(), RowGranularity.CLUSTER, analysisMetaData.referenceResolver()));
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        return process(node, analysis);
    }


    @Override
    public AnalyzedStatement visitUpdate(Update node, Analysis analysis) {
        StatementAnalysisContext statementAnalysisContext = new StatementAnalysisContext(
                analysis.parameterContext(), analysis.statementContext(), analysisMetaData, Operation.UPDATE);
        RelationAnalysisContext currentRelationContext = statementAnalysisContext.startRelation();
        AnalyzedRelation analyzedRelation = relationAnalyzer.analyze(node.relation(), statementAnalysisContext);

        FieldResolver fieldResolver = (FieldResolver) analyzedRelation;
        FieldProvider columnFieldProvider = new NameFieldProvider(analyzedRelation);
        ExpressionAnalyzer columnExpressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, analysis.parameterContext(), columnFieldProvider, fieldResolver);
        columnExpressionAnalyzer.setResolveFieldsOperation(Operation.UPDATE);

        assert Iterables.getOnlyElement(currentRelationContext.sources().values()) == analyzedRelation;
        ExpressionAnalyzer expressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, analysis.parameterContext(),
                    currentRelationContext.fieldProvider(), fieldResolver);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext(analysis.statementContext());

        int numNested = 1;
        if (analysis.parameterContext().numBulkParams() > 0) {
            numNested = analysis.parameterContext().numBulkParams();
        }

        WhereClauseAnalyzer whereClauseAnalyzer = null;
        if (analyzedRelation instanceof DocTableRelation) {
            whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, ((DocTableRelation) analyzedRelation));
        }
        TableInfo tableInfo = ((AbstractTableRelation) analyzedRelation).tableInfo();

        List<UpdateAnalyzedStatement.NestedAnalyzedStatement> nestedAnalyzedStatements = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            analysis.parameterContext().setBulkIdx(i);

            WhereClause whereClause = expressionAnalyzer.generateWhereClause(node.whereClause(), expressionAnalysisContext);
            if (whereClauseAnalyzer != null) {
                whereClause = whereClauseAnalyzer.analyze(whereClause, analysis.statementContext());
            }

            if (!whereClause.docKeys().isPresent() && Symbols.containsColumn(whereClause.query(), DocSysColumns.VERSION)) {
                throw VERSION_SEARCH_EX;
            }

            UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement =
                    new UpdateAnalyzedStatement.NestedAnalyzedStatement(whereClause);

            for (Assignment assignment : node.assignements()) {
                analyzeAssignment(
                        assignment,
                        nestedAnalyzedStatement,
                        tableInfo,
                        expressionAnalyzer,
                        columnExpressionAnalyzer,
                        expressionAnalysisContext
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
                                   ExpressionAnalyzer expressionAnalyzer,
                                   ExpressionAnalyzer columnExpressionAnalyzer,
                                   ExpressionAnalysisContext expressionAnalysisContext) {
        // unknown columns in strict objects handled in here
        Reference reference = (Reference) columnExpressionAnalyzer.normalize(
            columnExpressionAnalyzer.convert(node.columnName(), expressionAnalysisContext),
            expressionAnalysisContext.statementContext());

        final ColumnIdent ident = reference.info().ident().columnIdent();
        if (hasMatchingParent(tableInfo, reference.info(), IS_OBJECT_ARRAY)) {
            // cannot update fields of object arrays
            throw new IllegalArgumentException("Updating fields of object arrays is not supported");
        }
        Symbol value = expressionAnalyzer.normalize(
            expressionAnalyzer.convert(node.expression(), expressionAnalysisContext),
            expressionAnalysisContext.statementContext());
        try {
            value = valueNormalizer.normalizeInputForReference(value, reference, expressionAnalysisContext.statementContext());
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw new ColumnValidationException(ident.sqlFqn(), e);
        }

        nestedAnalyzedStatement.addAssignment(reference, value);
    }


    private boolean hasMatchingParent(TableInfo tableInfo, ReferenceInfo info, Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = tableInfo.getReferenceInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

}
