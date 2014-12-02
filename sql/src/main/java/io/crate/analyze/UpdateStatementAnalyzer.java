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
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.*;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Update;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.crate.planner.symbol.Field.unwrap;

public class UpdateStatementAnalyzer extends DefaultTraversalVisitor<AnalyzedStatement, Void> {

    private static final Predicate<ReferenceInfo> IS_OBJECT_ARRAY = new Predicate<ReferenceInfo>() {
        @Override
        public boolean apply(@Nullable ReferenceInfo input) {
            return input != null
                    && input.type().id() == ArrayType.ID
                    && ((ArrayType)input.type()).innerType().equals(DataTypes.OBJECT);
        }
    };

    private AnalysisMetaData analysisMetaData;
    private ParameterContext parameterContext;

    public UpdateStatementAnalyzer(AnalysisMetaData analysisMetaData, ParameterContext parameterContext) {
        this.analysisMetaData = analysisMetaData;
        this.parameterContext = parameterContext;
    }

    @Override
    public AnalyzedStatement visitUpdate(Update node, Void context) {
        RelationAnalyzer relationAnalyzer = new RelationAnalyzer(analysisMetaData, parameterContext);
        RelationAnalysisContext relationAnalysisContext = new RelationAnalysisContext();

        AnalyzedRelation analyzedRelation = relationAnalyzer.process(node.relation(), relationAnalysisContext);
        if (Relations.isReadOnly(analyzedRelation)) {
            throw new UnsupportedOperationException(String.format(
                    "relation \"%s\" is read-only and cannot be updated", analyzedRelation));
        }
        assert analyzedRelation instanceof TableRelation : "sourceRelation must be a TableRelation";
        TableInfo tableInfo = ((TableRelation) analyzedRelation).tableInfo();

        ExpressionAnalyzer expressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, parameterContext, relationAnalysisContext.sources());
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        int numNested = 1;
        if (parameterContext.bulkParameters.length > 0) {
            numNested = parameterContext.bulkParameters.length;
        }
        List<UpdateAnalyzedStatement.NestedAnalyzedStatement> nestedAnalyzedStatements = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            parameterContext.setBulkIdx(i);

            UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement = new UpdateAnalyzedStatement.NestedAnalyzedStatement(
                    expressionAnalyzer.generateWhereClause(node.whereClause(), expressionAnalysisContext));
            for (Assignment assignment : node.assignements()) {
                analyzeAssignment(
                        assignment,
                        nestedAnalyzedStatement,
                        tableInfo,
                        expressionAnalyzer,
                        expressionAnalysisContext
                );
            }
            nestedAnalyzedStatements.add(nestedAnalyzedStatement);
        }
        if (expressionAnalysisContext.hasSysExpressions) {
            throw new UnsupportedOperationException("Cannot use sys expressions in UPDATE statements");
        }

        return new UpdateAnalyzedStatement(analyzedRelation, nestedAnalyzedStatements);
    }

    public void analyzeAssignment(Assignment node,
                                  UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement,
                                  TableInfo tableInfo,
                                  ExpressionAnalyzer expressionAnalyzer,
                                  ExpressionAnalysisContext expressionAnalysisContext) {
        expressionAnalyzer.resolveWritableFields(true);
        // unknown columns in strict objects handled in here
        Reference reference = (Reference) expressionAnalyzer.normalize(
                unwrap(expressionAnalyzer.convert(node.columnName(), expressionAnalysisContext)));

        expressionAnalyzer.resolveWritableFields(false);
        final ColumnIdent ident = reference.info().ident().columnIdent();
        if (ident.name().startsWith("_")) {
            throw new IllegalArgumentException("Updating system columns is not allowed");
        }

        if (hasMatchingParent(tableInfo, reference.info(), IS_OBJECT_ARRAY)) {
            // cannot update fields of object arrays
            throw new IllegalArgumentException("Updating fields of object arrays is not supported");
        }

        // it's something that we can normalize to a literal
        Symbol value = expressionAnalyzer.normalize(
                unwrap(expressionAnalyzer.convert(node.expression(), expressionAnalysisContext)));
        Literal updateValue;
        try {
            updateValue = expressionAnalyzer.normalizeInputForReference(value, reference, true);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw new ColumnValidationException(ident.fqn(), e);
        }

        if (tableInfo.clusteredBy() != null) {
            ensureNotUpdated(ident, updateValue, tableInfo.clusteredBy(),
                    "Updating a clustered-by column is not supported");
        }
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            ensureNotUpdated(ident, updateValue, pkIdent, "Updating a primary key is not supported");
        }
        for (ColumnIdent partitionIdent : tableInfo.partitionedBy()) {
            ensureNotUpdated(ident, updateValue, partitionIdent, "Updating a partitioned-by column is not supported");
        }
        nestedAnalyzedStatement.addAssignment(reference, updateValue);
    }

    private void ensureNotUpdated(ColumnIdent columnUpdated,
                                  Literal newValue,
                                  ColumnIdent protectedColumnIdent,
                                  String errorMessage) {
        if (columnUpdated.equals(protectedColumnIdent)) {
            throw new IllegalArgumentException(errorMessage);
        }
        if (protectedColumnIdent.isChildOf(columnUpdated) &&
                !(newValue.valueType().equals(DataTypes.OBJECT)
                        && StringObjectMaps.fromMapByPath((Map) newValue.value(), protectedColumnIdent.path()) == null)) {
            throw new IllegalArgumentException(errorMessage);
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
