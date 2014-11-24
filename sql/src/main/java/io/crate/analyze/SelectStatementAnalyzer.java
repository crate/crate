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

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import io.crate.analyze.validator.GroupBySymbolValidator;
import io.crate.analyze.validator.HavingSymbolValidator;
import io.crate.analyze.validator.SelectSymbolValidator;
import io.crate.analyze.validator.SortSymbolValidator;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.exceptions.SQLParseException;
import io.crate.metadata.*;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class SelectStatementAnalyzer extends DataStatementAnalyzer<SelectAnalyzedStatement> {

    private final static AggregationSearcher AGGREGATION_SEARCHER = new AggregationSearcher();
    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final ReferenceResolver globalReferenceResolver;

    @Inject
    public SelectStatementAnalyzer(ReferenceInfos referenceInfos,
                                   Functions functions,
                                   ReferenceResolver globalReferenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.globalReferenceResolver = globalReferenceResolver;
    }


    @Override
    protected Symbol visitSelect(Select node, SelectAnalyzedStatement context) {
        context.outputSymbols(new ArrayList<Symbol>(node.getSelectItems().size()));
        context.outputNames(new ArrayList<String>(node.getSelectItems().size()));

        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected Symbol visitSingleColumn(SingleColumn node, SelectAnalyzedStatement context) {
        Symbol symbol = process(node.getExpression(), context);
        context.outputSymbols().add(symbol);

        if (node.getAlias().isPresent()) {
            context.addAlias(node.getAlias().get(), symbol);
        } else {
            context.addAlias(OutputNameFormatter.format(node.getExpression()), symbol);
        }

        return null;
    }

    @Override
    protected Symbol visitAllColumns(AllColumns node, SelectAnalyzedStatement context) {
        Symbol symbol;
        for (ReferenceInfo referenceInfo : context.table().columns()) {
            // ignore NOT_SUPPORTED columns
            if (referenceInfo.type() != DataTypes.NOT_SUPPORTED) {
                symbol = context.allocateReference(referenceInfo.ident());
                context.outputSymbols().add(symbol);
                context.addAlias(referenceInfo.ident().columnIdent().name(), symbol);
            }
        }

        return null;
    }

    protected Symbol visitQuerySpecification(QuerySpecification node, SelectAnalyzedStatement context) {
        // visit the from first, since this qualifies the select
        int numTables = node.getFrom() == null ? 0 : node.getFrom().size();
        if (numTables != 1) {
            throw new SQLParseException(
                    "Only exactly one table is allowed in the from clause, got: " + numTables
            );
        }
        process(node.getFrom().get(0), context);

        context.limit(intFromOptionalExpression(node.getLimit(), context.parameters()));
        context.offset(firstNonNull(
                intFromOptionalExpression(node.getOffset(), context.parameters()), 0));


        context.whereClause(generateWhereClause(node.getWhere(), context));

        if (!node.getGroupBy().isEmpty()) {
            context.selectFromFieldCache = true;
        }

        process(node.getSelect(), context);

        // validate select symbols
        SelectSymbolValidator.validate(context.outputSymbols(), context.selectFromFieldCache);

        if (!node.getGroupBy().isEmpty()) {
            analyzeGroupBy(node.getGroupBy(), context);
        }

        if (!node.getGroupBy().isEmpty() || context.hasAggregates()) {
            ensureNonAggregatesInGroupBy(context);
        }

        if (node.getSelect().isDistinct() && node.getGroupBy().isEmpty()) {
            rewriteGlobalDistinct(context);
        }

        if (node.getHaving().isPresent()) {
            if (node.getGroupBy().isEmpty() && !context.hasAggregates()) {
                throw new IllegalArgumentException("HAVING clause can only be used in GROUP BY or global aggregate queries");
            }
            processHavingClause(node.getHaving().get(), context);
        }

        if (node.getOrderBy().isEmpty()) {
            context.orderBy(OrderBy.NO_ORDER_BY);
        } else {
            context.orderBy(getOrderBy(node.getOrderBy(), context));
        }
        return null;
    }

    private void processHavingClause(Expression expression, SelectAnalyzedStatement context) {
        Symbol havingQuery = process(expression, context);
        HavingSymbolValidator.validate(havingQuery, context.groupBy());
        context.havingClause(havingQuery);
    }

    private OrderBy getOrderBy(List<SortItem> orderBy, SelectAnalyzedStatement context) {
        List<Symbol> sortSymbols = new ArrayList<>(orderBy.size());
        boolean[] reverseFlags = new boolean[orderBy.size()];
        Boolean[] nullsFirst = new Boolean[orderBy.size()];

        int i = 0;
        for (SortItem sortItem : orderBy) {
            sortSymbols.add(process(sortItem, context));
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
            i++;
        }
        return new OrderBy(sortSymbols, reverseFlags, nullsFirst);
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

    private void rewriteGlobalDistinct(SelectAnalyzedStatement context) {
        ArrayList<Symbol> groupBy = new ArrayList<>(context.outputSymbols().size());
        context.groupBy(groupBy);

        for (Symbol s : context.outputSymbols()) {
            if (s.symbolType() == SymbolType.DYNAMIC_REFERENCE) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("unknown column '%s' not allowed in a global DISTINCT", s));
            } else if (isAggregate(s)) {
                continue; // do not add aggregates
            }
            groupBy.add(s);
        }
    }

    @Nullable
    private Integer intFromOptionalExpression(Optional<Expression> limit, Object[] parameters) {
       if (limit.isPresent()) {
            Number number = ExpressionToNumberVisitor.convert(limit.get(), parameters);
            if (number instanceof Integer) {
                return (Integer) number;
            }
            return number.intValue();
        }
        return null;
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
                                                               SelectAnalyzedStatement context,
                                                               String clause) {
        Symbol symbol;
        if (expression instanceof QualifiedNameReference) {
            List<String> parts = ((QualifiedNameReference) expression).getName().getParts();
            if (parts.size() == 1) {
                symbol = context.symbolFromAlias(Iterables.getOnlyElement(parts));
                if (symbol != null) {
                    return symbol;
                }
            }
        }
        symbol = process(expression, context);
        if (symbol.symbolType().isValueSymbol()) {
            Literal longLiteral;
            try {
                longLiteral = Literal.convert(symbol, DataTypes.LONG);
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new UnsupportedOperationException(String.format(
                        "Cannot use %s in %s clause", SymbolFormatter.format(symbol), clause));
            }
            symbol = ordinalOutputReference(context.outputSymbols(), longLiteral, clause);
        }
        return symbol;
    }

    private void analyzeGroupBy(List<Expression> groupByExpressions, SelectAnalyzedStatement context) {
        List<Symbol> groupBy = new ArrayList<>(groupByExpressions.size());
        for (Expression expression : groupByExpressions) {
            Symbol s = symbolFromSelectOutputReferenceOrExpression(expression, context, "GROUP BY");
            GroupBySymbolValidator.validate(s);
            groupBy.add(s);
        }
        context.groupBy(groupBy);
    }

    private void ensureNonAggregatesInGroupBy(SelectAnalyzedStatement context) {
        for (Symbol symbol : context.outputSymbols()) {
            List<Symbol> groupBySymbols = context.groupBy();
            if (groupBySymbols == null || !groupBySymbols.contains(symbol)) {
                if (!isAggregate(symbol)) {
                    throw new IllegalArgumentException(
                            SymbolFormatter.format("column '%s' must appear in the GROUP BY clause or be used in an aggregation function",
                                    symbol));
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

    @Override
    protected Symbol visitSortItem(SortItem node, SelectAnalyzedStatement context) {
        Expression sortKey = node.getSortKey();
        Symbol sortSymbol = symbolFromSelectOutputReferenceOrExpression(sortKey, context, "ORDER BY");
        SortSymbolValidator.validate(sortSymbol, context.table.partitionedBy());
        return sortSymbol;
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new SelectAnalyzedStatement(referenceInfos, functions, parameterContext, globalReferenceResolver);
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






}
