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
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.types.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class InsertFromSubQueryAnalyzer extends AbstractInsertAnalyzer<Void> {

    private AnalysisMetaData analysisMetaData;
    private ParameterContext parameterContext;

    public InsertFromSubQueryAnalyzer(AnalysisMetaData analysisMetaData, ParameterContext parameterContext) {
        this.analysisMetaData = analysisMetaData;
        this.parameterContext = parameterContext;
    }

    @Override
    public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, Void context) {

        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node.table()));

        SelectStatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer(analysisMetaData, parameterContext);
        AnalyzedStatement statement = selectStatementAnalyzer.process(node.subQuery(), null);
        assert statement instanceof SelectAnalyzedStatement : "sub-query must be a SelectAnalyzedStatement";
        SelectAnalyzedStatement selectAnalyzedStatement = (SelectAnalyzedStatement) statement;

        InsertFromSubQueryAnalyzedStatement insertStatement =
                new InsertFromSubQueryAnalyzedStatement(selectAnalyzedStatement, tableInfo);

        // We forbid using limit/offset or order by until we've implemented ES paging support (aka 'scroll')
        if (selectAnalyzedStatement.isLimited() || selectAnalyzedStatement.orderBy().isSorted()) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not" +
                    "supported on insert using a sub-query");
        }

        int numInsertColumns = node.columns().size() == 0 ? tableInfo.columns().size() : node.columns().size();
        int maxInsertValues = Math.max(numInsertColumns, selectAnalyzedStatement.outputSymbols().size());
        handleInsertColumns(node, maxInsertValues, insertStatement);

        validateMatchingColumns(insertStatement, selectAnalyzedStatement);

        return insertStatement;
    }

    /**
     * validate that result columns from subquery match explicit insert columns
     * or complete table schema
     */
    private void validateMatchingColumns(InsertFromSubQueryAnalyzedStatement context, SelectAnalyzedStatement selectAnalyzedStatement) {
        List<Reference> insertColumns = context.columns();
        List<Symbol> sourceSymbols = selectAnalyzedStatement.outputSymbols();
        if (insertColumns.size() != sourceSymbols.size()) {
            throw new IllegalArgumentException("Number of columns in insert statement and subquery differ");
        }
        
        for (int i = 0; i < sourceSymbols.size(); i++) {
            Reference insertColumn = insertColumns.get(i);
            DataType targetType = insertColumn.valueType();
            Symbol sourceColumn = sourceSymbols.get(i);
            DataType sourceType = sourceColumn.valueType();

            if (!targetType.equals(sourceType)) {
                if (sourceType.isConvertableTo(targetType)) {
                    Function castFunction = new Function(
                            CastFunctionResolver.functionInfo(sourceType, targetType),
                            Arrays.asList(sourceColumn));
                    if (selectAnalyzedStatement.hasGroupBy()) {
                        replaceIfPresent(selectAnalyzedStatement.groupBy(), sourceColumn, castFunction);
                    }
                    if (selectAnalyzedStatement.orderBy().isSorted()) {
                        replaceIfPresent(selectAnalyzedStatement.orderBy().orderBySymbols(), sourceColumn, castFunction);
                    }
                    sourceSymbols.set(i, castFunction);
                } else {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Type of subquery column %s (%s) does not match is not convertable to the type of table column %s (%s)",
                            SymbolFormatter.format(sourceColumn),
                            sourceType,
                            insertColumn.info().ident().columnIdent().fqn(),
                            targetType
                    ));
                }
            }
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
}
