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
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.inject.Inject;

public class DeleteStatementAnalyzer extends AbstractStatementAnalyzer<Symbol, DeleteAnalyzedStatement> {

    final DataStatementAnalyzer<DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement> innerAnalyzer =
        new DataStatementAnalyzer<DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement>() {

            @Override
            public Symbol visitDelete(Delete node, DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement context) {
                process(node.getRelation(), context);
                context.whereClause(generateWhereClause(node.getWhere(), context));

                return null;
            }

            @Override
            public AnalyzedStatement newAnalysis(Analyzer.ParameterContext parameterContext) {
                return new UpdateAnalyzedStatement.NestedAnalyzedStatement(
                    referenceInfos, functions, parameterContext, globalReferenceResolver);
            }

            @Override
        protected Symbol visitTable(Table node, DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement context) {
            Preconditions.checkState(context.table() == null, "deleting multiple tables is not supported");
            context.editableTable(TableIdent.of(node));
            return null;
        }
    };

    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final ReferenceResolver globalReferenceResolver;

    @Inject
    public DeleteStatementAnalyzer(ReferenceInfos referenceInfos,
                                   Functions functions,
                                   ReferenceResolver globalReferenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.globalReferenceResolver = globalReferenceResolver;
    }

    @Override
    public Symbol visitDelete(Delete node, DeleteAnalyzedStatement context) {
        java.util.List<DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement> nestedAnalysis = context.nestedStatements;
        for (int i = 0, nestedAnalysisSize = nestedAnalysis.size(); i < nestedAnalysisSize; i++) {
            DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement nestedAnalysi = nestedAnalysis.get(i);
            context.parameterContext().setBulkIdx(i);
            innerAnalyzer.process(node, nestedAnalysi);
        }
        return null;
    }

    @Override
    public AnalyzedStatement newAnalysis(Analyzer.ParameterContext parameterContext) {
        return new DeleteAnalyzedStatement(referenceInfos, functions, parameterContext, globalReferenceResolver);
    }
}
