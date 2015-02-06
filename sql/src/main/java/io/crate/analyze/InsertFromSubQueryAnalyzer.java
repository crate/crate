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

public class InsertFromSubQueryAnalyzer extends AbstractInsertAnalyzer<InsertFromSubQueryAnalysis> {


    private final SelectStatementAnalyzer subQueryAnalyzer;
    private final Functions functions;
    private final ReferenceInfos referenceInfos;
    private final ReferenceResolver globalReferenceResolver;

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
    public Symbol visitInsertFromSubquery(InsertFromSubquery node, InsertFromSubQueryAnalysis context) {
        node.table().accept(this, context); // table existence check happening here

        process(node.subQuery(), context);

        // We forbid using limit/offset or order by until we've implemented ES paging support (aka 'scroll')
        if (context.subQueryAnalysis().isLimited() || context.subQueryAnalysis().isSorted()) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not" +
                    "supported on insert using a sub-query");
        }

        int numInsertColumns = node.columns().size() == 0 ? context.table().columns().size() : node.columns().size();
        int maxInsertValues = Math.max(numInsertColumns, context.getSubQueryColumns().size());
        handleInsertColumns(node, maxInsertValues, context);

        validateMatchingColumns(node, context);

        return null;
    }

    @Override
    protected Symbol visitQuery(Query node, InsertFromSubQueryAnalysis context) {
        return subQueryAnalyzer.visitQuery(node, context.subQueryAnalysis());
    }

    /**
     * validate that result columns from subquery match explicit insert columns
     * or complete table schema
     * @param node
     * @param context
     */
    private void validateMatchingColumns(InsertFromSubquery node, InsertFromSubQueryAnalysis context) {
        List<Reference> insertColumns = context.columns();
        List<Symbol> subQueryColumns = context.getSubQueryColumns();

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

            if (!(subQueryColumn instanceof DataTypeSymbol)) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("Invalid column expression in subquery: '%s'", subQueryColumn)
                );
            }
            DataType subQueryColumnType = ((DataTypeSymbol) subQueryColumn).valueType();

            if (!subQueryColumnType.equals(insertColumn.valueType())) {
                if (!subQueryColumnType.isConvertableTo(insertColumn.valueType())) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "Type of subquery column %s (%s) does not match /" +
                                            " is not convertable to the type of table column %s (%s)",
                                    SymbolFormatter.format(subQueryColumn),
                                    ((DataTypeSymbol) subQueryColumn).valueType(),
                                    insertColumn.info().ident().columnIdent().fqn(),
                                    insertColumn.valueType()
                            ));
                } else {
                    // replace column by `toX` function
                    FunctionInfo functionInfo = CastFunctionResolver.functionInfo(subQueryColumnType, insertColumn.valueType());
                    Function function = context.allocateFunction(functionInfo, Arrays.asList(subQueryColumn));
                    if (context.subQueryAnalysis().hasGroupBy()) {
                        int groupByIdx = context.subQueryAnalysis().groupBy().indexOf(subQueryColumn);
                        if (groupByIdx != -1) {
                            context.subQueryAnalysis().groupBy().set(groupByIdx, function);
                        }
                    }
                    if (context.subQueryAnalysis().sortSymbols() != null) {
                        int sortSymbolIdx = context.subQueryAnalysis().sortSymbols().indexOf(subQueryColumn);
                        if (sortSymbolIdx != -1) {
                            context.subQueryAnalysis().sortSymbols().set(sortSymbolIdx, function);
                        }
                    }
                    subQueryColumns.set(idx, function);
                }
            }
            idx++;
        }
        // congrats! valid statement
    }

    @Override
    public Analysis newAnalysis(ParameterContext parameterContext) {
        return new InsertFromSubQueryAnalysis(referenceInfos, functions, parameterContext, globalReferenceResolver);
    }
}
