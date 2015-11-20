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
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
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
import java.util.Map;


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


    @Inject
    public UpdateStatementAnalyzer(AnalysisMetaData analysisMetaData,
                                   RelationAnalyzer relationAnalyzer) {
        this.analysisMetaData = analysisMetaData;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return process(node, analysis);
    }


    @Override
    public AnalyzedStatement visitUpdate(Update node, Analysis analysis) {
        RelationAnalysisContext relationAnalysisContext = new RelationAnalysisContext(
                analysis.parameterContext(), analysisMetaData);
        AnalyzedRelation analyzedRelation = relationAnalyzer.analyze(node.relation(), relationAnalysisContext);
        if (Relations.isReadOnly(analyzedRelation)) {
            throw new UnsupportedOperationException(String.format(
                    "relation \"%s\" is read-only and cannot be updated", analyzedRelation));
        }
        assert analyzedRelation instanceof DocTableRelation : "sourceRelation must be a DocTableRelation";
        DocTableRelation tableRelation = ((DocTableRelation) analyzedRelation);

        FieldProvider columnFieldProvider = new NameFieldProvider(analyzedRelation);
        ExpressionAnalyzer columnExpressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, analysis.parameterContext(), columnFieldProvider, tableRelation);
        columnExpressionAnalyzer.resolveWritableFields(true);

        assert Iterables.getOnlyElement(relationAnalysisContext.sources().values()) == tableRelation;
        FieldProvider fieldProvider = new FullQualifedNameFieldProvider(relationAnalysisContext.sources());
        ExpressionAnalyzer expressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, analysis.parameterContext(), fieldProvider, tableRelation);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        int numNested = 1;
        if (analysis.parameterContext().bulkParameters.length > 0) {
            numNested = analysis.parameterContext().bulkParameters.length;
        }
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
        List<UpdateAnalyzedStatement.NestedAnalyzedStatement> nestedAnalyzedStatements = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            analysis.parameterContext().setBulkIdx(i);

            WhereClause whereClause = expressionAnalyzer.generateWhereClause(node.whereClause(), expressionAnalysisContext);
            whereClause = whereClauseAnalyzer.analyze(whereClause);

            if (!whereClause.docKeys().isPresent() && Symbols.containsColumn(whereClause.query(), DocSysColumns.VERSION)) {
                throw VERSION_SEARCH_EX;
            }

            UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement =
                    new UpdateAnalyzedStatement.NestedAnalyzedStatement(whereClause);



            for (Assignment assignment : node.assignements()) {
                analyzeAssignment(
                        assignment,
                        nestedAnalyzedStatement,
                        tableRelation,
                        expressionAnalyzer,
                        columnExpressionAnalyzer,
                        expressionAnalysisContext
                );
            }
            nestedAnalyzedStatements.add(nestedAnalyzedStatement);
        }
        return new UpdateAnalyzedStatement(tableRelation, nestedAnalyzedStatements);
    }

    public void analyzeAssignment(Assignment node,
                                  UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement,
                                  DocTableRelation tableRelation,
                                  ExpressionAnalyzer expressionAnalyzer,
                                  ExpressionAnalyzer columnExpressionAnalyzer,
                                  ExpressionAnalysisContext expressionAnalysisContext) {
        // unknown columns in strict objects handled in here
        Reference reference = (Reference) columnExpressionAnalyzer.normalize(
                columnExpressionAnalyzer.convert(node.columnName(), expressionAnalysisContext));

        final ColumnIdent ident = reference.info().ident().columnIdent();
        if (ident.name().startsWith("_")) {
            throw new IllegalArgumentException("Updating system columns is not allowed");
        }

        if (hasMatchingParent(tableRelation.tableInfo(), reference.info(), IS_OBJECT_ARRAY)) {
            // cannot update fields of object arrays
            throw new IllegalArgumentException("Updating fields of object arrays is not supported");
        }
        Symbol value = expressionAnalyzer.normalize(
                expressionAnalyzer.convert(node.expression(), expressionAnalysisContext));
        try {
            value = expressionAnalyzer.normalizeInputForReference(value, reference, expressionAnalysisContext);
            ensureUpdateIsAllowed(tableRelation.tableInfo(), ident, value);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw new ColumnValidationException(ident.sqlFqn(), e);
        }

        nestedAnalyzedStatement.addAssignment(reference, value);
    }

    public static void ensureUpdateIsAllowed(DocTableInfo tableInfo, ColumnIdent column, Symbol value) {
        if (tableInfo.clusteredBy() != null) {
            ensureNotUpdated(column, value, tableInfo.clusteredBy(),
                    "Updating a clustered-by column is not supported");
        }
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            ensureNotUpdated(column, value, pkIdent, "Updating a primary key is not supported");
        }
        for (ColumnIdent partitionIdent : tableInfo.partitionedBy()) {
            ensureNotUpdated(column, value, partitionIdent, "Updating a partitioned-by column is not supported");
        }
    }

    private static void ensureNotUpdated(ColumnIdent columnUpdated,
                                         Symbol newValue,
                                         ColumnIdent protectedColumnIdent,
                                         String errorMessage) {
        if (columnUpdated.equals(protectedColumnIdent)) {
            throw new UnsupportedOperationException(errorMessage);
        }
        if (protectedColumnIdent.isChildOf(columnUpdated)) {
            if (newValue.valueType().equals(DataTypes.OBJECT)
                    && newValue.symbolType().isValueSymbol()
                    && StringObjectMaps.fromMapByPath((Map) ((Literal) newValue).value(), protectedColumnIdent.path()) == null) {
                return;
            }
            throw new UnsupportedOperationException(errorMessage);
        }
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