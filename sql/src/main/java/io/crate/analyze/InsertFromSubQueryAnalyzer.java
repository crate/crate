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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.sql.tree.Query;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class InsertFromSubQueryAnalyzer extends AbstractInsertAnalyzer<InsertFromSubQueryAnalyzedStatement> {

    private final SelectStatementAnalyzer subQueryAnalyzer;
    private final Functions functions;
    private final ReferenceInfos referenceInfos;
    private final ReferenceResolver globalReferenceResolver;

    private final SelectAnalyzedStatementRelationVisitor relationVisitor = new SelectAnalyzedStatementRelationVisitor();


    @Inject
    public InsertFromSubQueryAnalyzer(SelectStatementAnalyzer selectStatementAnalyzer,
                                      Functions functions,
                                      ReferenceInfos referenceInfos,
                                      ReferenceResolver globalReferenceResolver) {
        this.subQueryAnalyzer = selectStatementAnalyzer;
        this.functions = functions;
        this.referenceInfos = referenceInfos;
        this.globalReferenceResolver = globalReferenceResolver;
    }

    @Override
    public Void visitInsertFromSubquery(InsertFromSubquery node, InsertFromSubQueryAnalyzedStatement context) {
        node.table().accept(this, context); // table existence check happening here

        process(node.subQuery(), context);

        Context relationCtx = new Context();
        relationVisitor.process(context.subQueryRelation(), relationCtx);
        SelectAnalyzedStatement selectAnalyzedStatement = relationCtx.selectAnalyzedStatement;

        // We forbid using limit/offset or order by until we've implemented ES paging support (aka 'scroll')
        if (selectAnalyzedStatement.isLimited() || selectAnalyzedStatement.orderBy().isSorted()) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not" +
                    "supported on insert using a sub-query");
        }

        int numInsertColumns = node.columns().size() == 0 ? context.table().columns().size() : node.columns().size();
        int maxInsertValues = Math.max(numInsertColumns, selectAnalyzedStatement.outputSymbols().size());
        handleInsertColumns(node, maxInsertValues, context);

        validateMatchingColumns(context, selectAnalyzedStatement);

        return null;
    }

    @Override
    protected Void visitQuery(Query node, InsertFromSubQueryAnalyzedStatement context) {
        subQueryAnalyzer.process(node, (SelectAnalyzedStatement) context.subQueryRelation());
        return null;
    }

    /**
     * validate that result columns from subquery match explicit insert columns
     * or complete table schema
     */
    private void validateMatchingColumns(InsertFromSubQueryAnalyzedStatement context, SelectAnalyzedStatement selectAnalyzedStatement) {
        List<Reference> insertColumns = context.columns();
        List<Symbol> subQueryColumns = selectAnalyzedStatement.outputSymbols();

        if (insertColumns.size() != subQueryColumns.size()) {
            throw new IllegalArgumentException("Number of columns in insert statement and subquery differ");
        }

        Iterator<Reference> insertColumnsIter = insertColumns.iterator();
        Iterator<Symbol> subQueryColumnsIter = subQueryColumns.iterator();
        Reference insertColumn;
        Symbol subQueryColumn;
        int idx = 0;
        while (insertColumnsIter.hasNext()) {

            insertColumn = insertColumnsIter.next();
            subQueryColumn = subQueryColumnsIter.next();

            DataType subQueryColumnType = subQueryColumn.valueType();

            if (subQueryColumnType != insertColumn.valueType()) {
                if (!subQueryColumnType.isConvertableTo(insertColumn.valueType())) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "Type of subquery column %s (%s) does not match /" +
                                            " is not convertable to the type of table column %s (%s)",
                                    SymbolFormatter.format(subQueryColumn),
                                    subQueryColumn.valueType(),
                                    insertColumn.info().ident().columnIdent().fqn(),
                                    insertColumn.valueType()
                            ));
                } else {
                    // replace column by `toX` function
                    FunctionInfo functionInfo = CastFunctionResolver.functionInfo(subQueryColumnType, insertColumn.valueType());
                    Function function = context.allocateFunction(functionInfo, Arrays.asList(subQueryColumn));
                    if (selectAnalyzedStatement.hasGroupBy()) {
                        replaceIfPresent(selectAnalyzedStatement.groupBy(), subQueryColumn, function);
                    }
                    if (selectAnalyzedStatement.orderBy().isSorted()) {
                        replaceIfPresent(selectAnalyzedStatement.orderBy().orderBySymbols(), subQueryColumn, function);
                    }
                    subQueryColumns.set(idx, function);
                }
            }
            idx++;
        }
        // congrats! valid statement
    }

    private void replaceIfPresent(List<Symbol> symbols, Symbol oldSymbol, Symbol newSymbol) {
        assert symbols != null : "symbols must not be null";
        int i = symbols.indexOf(oldSymbol);
        if (i != -1) {
            symbols.set(i, newSymbol);
        }
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new InsertFromSubQueryAnalyzedStatement(referenceInfos, functions, parameterContext, globalReferenceResolver);
    }

    private static class Context {
        SelectAnalyzedStatement selectAnalyzedStatement;
    }

    private static class SelectAnalyzedStatementRelationVisitor extends RelationVisitor<Context, Void> {

        @Override
        public Void visitSelectAnalyzedStatement(SelectAnalyzedStatement selectAnalyzedStatement, Context context) {
            context.selectAnalyzedStatement = selectAnalyzedStatement;
            return null;
        }

        @Override
        public Void visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            throw new UnsupportedOperationException(String.format(
                    "relation \"%s\" is not supported in the query part of INSERT BY QUERY", relation));
        }
    }
}
