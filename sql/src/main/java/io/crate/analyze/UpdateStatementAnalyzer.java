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

import com.google.common.base.Preconditions;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.*;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.Update;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class UpdateStatementAnalyzer extends AbstractStatementAnalyzer<Symbol, UpdateAnalyzedStatement> {

    final DataStatementAnalyzer<UpdateAnalyzedStatement.NestedAnalyzedStatement> innerAnalyzer =
            new DataStatementAnalyzer<UpdateAnalyzedStatement.NestedAnalyzedStatement>() {

                @Override
                public Symbol visitUpdate(Update node, UpdateAnalyzedStatement.NestedAnalyzedStatement context) {
                    process(node.relation(), context);
                    for (Assignment assignment : node.assignements()) {
                        process(assignment, context);
                    }
                    context.whereClause(generateWhereClause(node.whereClause(), context));
                    return null;
                }

                @Override
                public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
                    return new UpdateAnalyzedStatement.NestedAnalyzedStatement(
                        referenceInfos, functions, parameterContext, globalReferenceResolver);
                }

                @Override
                protected Symbol visitTable(Table node, UpdateAnalyzedStatement.NestedAnalyzedStatement context) {
                    Preconditions.checkState(context.table() == null, "updating multiple tables is not supported");
                    context.editableTable(TableIdent.of(node));
                    return null;
                }

                @Override
                public Symbol visitAssignment(Assignment node, UpdateAnalyzedStatement.NestedAnalyzedStatement context) {
                    // unknown columns in strict objects handled in here
                    Reference reference = (Reference)process(node.columnName(), context);
                    final ColumnIdent ident = reference.info().ident().columnIdent();
                    if (ident.name().startsWith("_")) {
                        throw new IllegalArgumentException("Updating system columns is not allowed");
                    }
                    if (context.hasMatchingParent(reference.info(), UpdateAnalyzedStatement.NestedAnalyzedStatement.IS_OBJECT_ARRAY)) {
                        // cannot update fields of object arrays
                        throw new IllegalArgumentException("Updating fields of object arrays is not supported");
                    }

                    // it's something that we can normalize to a literal
                    Symbol value = process(node.expression(), context);
                    Literal updateValue;
                    try {
                        updateValue = context.normalizeInputForReference(value, reference, true);
                    } catch(IllegalArgumentException|UnsupportedOperationException e) {
                        throw new ColumnValidationException(ident.sqlFqn(), e);
                    }

                    if (context.table.clusteredBy() != null) {
                        ensureNotUpdated(ident, updateValue, context.table.clusteredBy(),
                                "Updating a clustered-by column is not supported");
                    }

                    for (ColumnIdent pkIdent : context.table().primaryKey()) {
                        ensureNotUpdated(ident, updateValue, pkIdent, "Updating a primary key is not supported");
                    }
                    for (ColumnIdent partitionIdent : context.table.partitionedBy()) {
                        ensureNotUpdated(ident, updateValue, partitionIdent, "Updating a partitioned-by column is not supported");
                    }

                    context.addAssignment(reference, updateValue);
                    return null;
                }

                @Override
                protected Symbol visitQualifiedNameReference(QualifiedNameReference node, UpdateAnalyzedStatement.NestedAnalyzedStatement context) {
                    ReferenceIdent ident = context.getReference(node.getName());
                    return context.allocateReference(ident, true);
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
    };
    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final ReferenceResolver globalReferenceResolver;

    @Inject
    public UpdateStatementAnalyzer(ReferenceInfos referenceInfos,
                                   Functions functions,
                                   ReferenceResolver globalReferenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.globalReferenceResolver = globalReferenceResolver;
    }

    @Override
    public Symbol visitUpdate(Update node, UpdateAnalyzedStatement context) {
        java.util.List<UpdateAnalyzedStatement.NestedAnalyzedStatement> nestedAnalysisList = context.nestedAnalysisList;
        for (int i = 0, nestedAnalysisListSize = nestedAnalysisList.size(); i < nestedAnalysisListSize; i++) {
            UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis = nestedAnalysisList.get(i);
            context.parameterContext().setBulkIdx(i);
            innerAnalyzer.process(node, nestedAnalysis);
        }
        return null;
    }

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, UpdateAnalyzedStatement context) {
        ReferenceIdent ident = context.getReference(node.getName());
        return context.allocateReference(ident, true);
    }


    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new UpdateAnalyzedStatement(referenceInfos, functions, parameterContext, globalReferenceResolver);
    }
}
