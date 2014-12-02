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

package io.crate.analyze.relations;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.crate.analyze.*;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.analyze.validator.GroupBySymbolValidator;
import io.crate.analyze.validator.HavingSymbolValidator;
import io.crate.analyze.validator.SortSymbolValidator;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.crate.planner.symbol.Field.unwrap;

public class RelationAnalyzer extends DefaultTraversalVisitor<AnalyzedRelation, RelationAnalysisContext> {

    private final static AggregationSearcher AGGREGATION_SEARCHER = new AggregationSearcher();

    private final AnalysisMetaData analysisMetaData;
    private final ParameterContext parameterContext;

    private ExpressionAnalyzer expressionAnalyzer;
    private ExpressionAnalysisContext expressionAnalysisContext;

    public RelationAnalyzer(AnalysisMetaData analysisMetaData,
                            ParameterContext parameterContext) {
        this.analysisMetaData = analysisMetaData;
        this.parameterContext = parameterContext;
    }

    @Override
    protected AnalyzedRelation visitQuerySpecification(QuerySpecification node, RelationAnalysisContext context) {
        if (node.getFrom() == null) {
            throw new IllegalArgumentException("FROM clause is missing.");
        }
        for (Relation relation : node.getFrom()) {
            process(relation, context);
        }
        if (context.sources().size() != 1) {
            throw new UnsupportedOperationException
                    ("Only exactly one table is allowed in the FROM clause, got: " + context.sources().size());
        }
        expressionAnalyzer = new ExpressionAnalyzer(analysisMetaData, parameterContext, context.sources());
        expressionAnalysisContext = new ExpressionAnalysisContext();

        WhereClause whereClause = analyzeWhere(node.getWhere());
        SelectAnalyzer.SelectAnalysis selectAnalysis = SelectAnalyzer.analyzeSelect(
                node.getSelect(),
                context.sources(),
                expressionAnalyzer,
                expressionAnalysisContext,
                !node.getGroupBy().isEmpty()
        );

        List<Symbol> groupBy = analyzeGroupBy(selectAnalysis, node.getGroupBy());

        if (!node.getGroupBy().isEmpty() || expressionAnalysisContext.hasAggregates) {
            ensureNonAggregatesInGroupBy(selectAnalysis.outputSymbols(), groupBy);
        }
        if (node.getSelect().isDistinct() && groupBy.isEmpty()) {
            groupBy = rewriteGlobalDistinct(selectAnalysis.outputSymbols());
        }

        OrderBy orderBy = analyzeOrderBy(selectAnalysis, node.getOrderBy());
        Symbol having = analyzeHaving(node.getHaving(), groupBy, expressionAnalysisContext.hasAggregates);
        Integer limit = expressionAnalyzer.integerFromExpression(node.getLimit());
        int offset = firstNonNull(expressionAnalyzer.integerFromExpression(node.getOffset()), 0);

        return new SelectAnalyzedStatement(
                selectAnalysis.outputNames(),
                selectAnalysis.outputSymbols(),
                context.sources(),
                whereClause,
                groupBy,
                orderBy,
                having,
                limit,
                offset,
                expressionAnalysisContext.hasSysExpressions,
                expressionAnalysisContext.hasAggregates
        );
    }

    private List<Symbol> rewriteGlobalDistinct(List<Symbol> outputSymbols) {
        List<Symbol> groupBy = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            if (unwrap(symbol).symbolType() == SymbolType.DYNAMIC_REFERENCE) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("unknown column '%s' not allowed in a global DISTINCT", symbol));
            } else if (isAggregate(symbol)) {
                continue; // do not add aggregates
            }
            groupBy.add(symbol);
        }
        return groupBy;
    }

    private void ensureNonAggregatesInGroupBy(List<Symbol> outputSymbols, List<Symbol> groupBy) throws IllegalArgumentException {
        for (Symbol output : outputSymbols) {
            if (groupBy == null || !groupBy.contains(output)) {
                if (!isAggregate(output)) {
                    throw new IllegalArgumentException(
                            SymbolFormatter.format("column '%s' must appear in the GROUP BY clause " +
                                            "or be used in an aggregation function", output));
                }
            }
        }
    }

    private boolean isAggregate(Symbol s) {
        if (s.symbolType() == SymbolType.FUNCTION) {
            if (((Function) s).info().type() == FunctionInfo.Type.AGGREGATE) {
                return true;
            }
            AggregationSearcherContext searcherContext = new AggregationSearcherContext();
            AGGREGATION_SEARCHER.process(s, searcherContext);
            return searcherContext.found;
        }
        return false;
    }

    static class AggregationSearcherContext {
        boolean found = false;
    }

    static class AggregationSearcher extends SymbolVisitor<AggregationSearcherContext, Void> {

        @Override
        public Void visitFunction(Function symbol, AggregationSearcherContext context) {
            if (symbol.info().type() == FunctionInfo.Type.AGGREGATE) {
                context.found = true;
            } else {
                for (Symbol argument : symbol.arguments()) {
                    process(argument, context);
                }
            }
            return null;
        }

        @Override
        public Void visitAggregation(Aggregation symbol, AggregationSearcherContext context) {
            context.found = true;
            return null;
        }
    }

    private OrderBy analyzeOrderBy(SelectAnalyzer.SelectAnalysis selectAnalysis, List<SortItem> orderBy) {
        int size = orderBy.size();
        List<Symbol> symbols = new ArrayList<>(size);
        boolean[] reverseFlags = new boolean[size];
        Boolean[] nullsFirst = new Boolean[size];

        for (int i = 0; i < size; i++) {
            SortItem sortItem = orderBy.get(i);
            Expression sortKey = sortItem.getSortKey();
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(sortKey, selectAnalysis, "ORDER BY");
            SortSymbolValidator.validate(symbol, null);

            symbols.add(symbol);
            switch (sortItem.getNullOrdering()) {
                case FIRST:
                    nullsFirst[i] = true;
                    break;
                case LAST:
                    nullsFirst[i] = false;
                    break;
                case UNDEFINED:
                    nullsFirst[i] = null;
                    break;
            }
            reverseFlags[i] = sortItem.getOrdering() == SortItem.Ordering.DESCENDING;
        }
        return new OrderBy(symbols, reverseFlags, nullsFirst);
    }

    private List<Symbol> analyzeGroupBy(SelectAnalyzer.SelectAnalysis selectAnalysis, List<Expression> groupBy) {
        List<Symbol> groupBySymbols = new ArrayList<>(groupBy.size());
        for (Expression expression : groupBy) {
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(expression, selectAnalysis, "GROUP BY");
            GroupBySymbolValidator.validate(symbol);
            groupBySymbols.add(symbol);
        }
        return groupBySymbols;
    }

    private Symbol analyzeHaving(Optional<Expression> having, List<Symbol> groupBy, boolean hasAggregates) {
        if (having.isPresent()) {
            if (!hasAggregates && groupBy.isEmpty()) {
                throw new IllegalArgumentException("HAVING clause can only be used in GROUP BY or global aggregate queries");
            }
            Symbol symbol = expressionAnalyzer.convert(having.get(), expressionAnalysisContext);
            HavingSymbolValidator.validate(symbol, groupBy);
            return expressionAnalyzer.normalize(symbol);
        }
        return null;
    }

    private WhereClause analyzeWhere(Optional<Expression> where) {
        if (!where.isPresent()) {
            return WhereClause.MATCH_ALL;
        }
        return new WhereClause(expressionAnalyzer.normalize(expressionAnalyzer.convert(where.get(), expressionAnalysisContext)));
    }


    /**
     * <h2>resolve expression by also taking alias and ordinal-reference into account</h2>
     *
     * <p>
     *     in group by or order by clauses it is possible to reference anything in the
     *     select list by using a number or alias
     * </p>
     *
     * These are allowed:
     * <pre>
     *     select name as n  ... order by n
     *     select name  ... order by 1
     *     select name ... order by other_column
     * </pre>
     */
    private Symbol symbolFromSelectOutputReferenceOrExpression(Expression expression,
                                                               SelectAnalyzer.SelectAnalysis selectAnalysis,
                                                               String clause) {
        Symbol symbol;
        if (expression instanceof QualifiedNameReference) {
            List<String> parts = ((QualifiedNameReference) expression).getName().getParts();
            if (parts.size() == 1) {
                symbol = getOneOrAmbiguous(selectAnalysis.outputMultiMap(), Iterables.getOnlyElement(parts));
                if (symbol != null) {
                    return symbol;
                }
            }
        }
        symbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);
        if (symbol.symbolType().isValueSymbol()) {
            Literal longLiteral;
            try {
                longLiteral = io.crate.planner.symbol.Literal.convert(symbol, DataTypes.LONG);
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new UnsupportedOperationException(String.format(
                        "Cannot use %s in %s clause", SymbolFormatter.format(symbol), clause));
            }
            symbol = ordinalOutputReference(selectAnalysis.outputSymbols(), longLiteral, clause);
        }
        return symbol;
    }

    private Symbol ordinalOutputReference(List<Symbol> outputSymbols, Literal longLiteral, String clauseName) {
        assert longLiteral.valueType().equals(DataTypes.LONG) : "longLiteral must have valueType long";
        int idx = ((Long) longLiteral.value()).intValue() - 1;
        if (idx < 0) {
            throw new IllegalArgumentException(String.format(
                    "%s position %s is not in select list", clauseName, idx + 1));
        }
        try {
            return outputSymbols.get(idx);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(
                    "%s position %s is not in select list", clauseName, idx + 1));
        }
    }

    @Nullable
    private Symbol getOneOrAmbiguous(Multimap<String, Symbol> selectList, String key) throws AmbiguousColumnAliasException {
        Collection<Symbol> symbols = selectList.get(key);
        if (symbols.size() > 1) {
            throw new AmbiguousColumnAliasException(key);
        }
        if (symbols.isEmpty()) {
            return null;
        }
        return symbols.iterator().next();
    }

    @Override
    protected AnalyzedRelation visitAliasedRelation(AliasedRelation node, RelationAnalysisContext context) {
        AnalyzedRelation childRelation = process(node.getRelation(), new RelationAnalysisContext());
        context.addSourceRelation(node.getAlias(), childRelation);
        return childRelation;
    }

    @Override
    protected AnalyzedRelation visitTable(Table node, RelationAnalysisContext context) {
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node));
        TableRelation tableRelation = new TableRelation(tableInfo);
        context.addSourceRelation(tableInfo.schemaInfo().name(), tableInfo.ident().name(), tableRelation);
        return tableRelation;
    }
}
