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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SelectStatementAnalyzer extends DataStatementAnalyzer<SelectAnalysis> {

    private final static AggregationSearcher aggregationSearcher = new AggregationSearcher();
    private final static SortSymbolValidator sortSymbolValidator = new SortSymbolValidator();
    private final static GroupBySymbolValidator groupBySymbolValidator = new GroupBySymbolValidator();
    private final static SelectSymbolValidator selectSymbolVisitor = new SelectSymbolValidator();
    private final static HavingSymbolValidator havingSymbolValidator = new HavingSymbolValidator();
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
    protected Symbol visitSelect(Select node, SelectAnalysis context) {
        context.outputSymbols(new ArrayList<Symbol>(node.getSelectItems().size()));
        context.outputNames(new ArrayList<String>(node.getSelectItems().size()));

        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected Symbol visitSingleColumn(SingleColumn node, SelectAnalysis context) {
        Symbol symbol = process(node.getExpression(), context);
        if (symbol.symbolType() == SymbolType.PARAMETER) {
            //convert to Literal
            symbol = Literal.fromParameter((Parameter)symbol);
        }
        context.outputSymbols().add(symbol);

        if (node.getAlias().isPresent()) {
            context.addAlias(node.getAlias().get(), symbol);
        } else {
            context.addAlias(outputNameFormatter.process(node.getExpression(), null), symbol);
        }

        return null;
    }

    @Override
    protected Symbol visitAllColumns(AllColumns node, SelectAnalysis context) {
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

    protected Symbol visitQuerySpecification(QuerySpecification node, SelectAnalysis context) {
        // visit the from first, since this qualifies the select
        int numTables = node.getFrom() == null ? 0 : node.getFrom().size();
        if (numTables != 1) {
            throw new SQLParseException(
                    "Only exactly one table is allowed in the from clause, got: " + numTables
            );
        }
        process(node.getFrom().get(0), context);

        context.limit(intFromOptionalExpression(node.getLimit(), context.parameters()));
        context.offset(Objects.firstNonNull(
                intFromOptionalExpression(node.getOffset(), context.parameters()), 0));


        context.whereClause(generateWhereClause(node.getWhere(), context));
        if(context.whereClause().version().isPresent()){
            throw new UnsupportedFeatureException("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        }
        if (!node.getGroupBy().isEmpty()) {
            context.selectFromFieldCache = true;
        }

        process(node.getSelect(), context);

        // validate select symbols
        for (Symbol symbol : context.outputSymbols()) {
            selectSymbolVisitor.process(symbol, new SelectSymbolValidator.SelectContext(context.selectFromFieldCache));
        }

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

        if (node.getOrderBy().size() > 0) {
            addSorting(node.getOrderBy(), context);
        }
        return null;
    }

    private void processHavingClause(Expression expression, SelectAnalysis context) {
        Symbol havingQuery = process(expression, context);

        // validate having symbols
        HavingSymbolValidator.HavingContext havingContext = new HavingSymbolValidator.HavingContext(context.groupBy());
        havingSymbolValidator.process(havingQuery, havingContext);

        context.havingClause(havingQuery);
    }

    private void addSorting(List<SortItem> orderBy, SelectAnalysis context) {
        List<Symbol> sortSymbols = new ArrayList<>(orderBy.size());
        context.reverseFlags(new boolean[orderBy.size()]);
        context.nullsFirst(new Boolean[orderBy.size()]);

        int i = 0;
        for (SortItem sortItem : orderBy) {
            sortSymbols.add(process(sortItem, context));
            switch (sortItem.getNullOrdering()) {
                case FIRST:
                    context.nullsFirst()[i] = true;
                    break;
                case LAST:
                    context.nullsFirst()[i] = false;
                    break;
                case UNDEFINED:
                    context.nullsFirst()[i] = null;
                    break;
            }
            context.reverseFlags()[i] = sortItem.getOrdering() == SortItem.Ordering.DESCENDING;
            i++;
        }
        context.sortSymbols(sortSymbols);
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

    private void rewriteGlobalDistinct(SelectAnalysis context) {
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
                                                               SelectAnalysis context,
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
                longLiteral = Literal.toLiteral(symbol, DataTypes.LONG);
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new UnsupportedOperationException(String.format(
                        "Cannot use %s in %s clause", SymbolFormatter.format(symbol), clause));
            }
            symbol = ordinalOutputReference(context.outputSymbols(), longLiteral, clause);
        }
        return symbol;
    }

    private void analyzeGroupBy(List<Expression> groupByExpressions, SelectAnalysis context) {
        List<Symbol> groupBy = new ArrayList<>(groupByExpressions.size());
        for (Expression expression : groupByExpressions) {
            Symbol s = symbolFromSelectOutputReferenceOrExpression(expression, context, "GROUP BY");
            groupBySymbolValidator.process(s, null);
            groupBy.add(s);
        }
        context.groupBy(groupBy);
    }

    private void ensureNonAggregatesInGroupBy(SelectAnalysis context) {
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
            aggregationSearcher.process(s, searcherContext);
            return searcherContext.found;
        }
        return false;
    }

    @Override
    protected Symbol visitSortItem(SortItem node, SelectAnalysis context) {
        Expression sortKey = node.getSortKey();
        Symbol sortSymbol = symbolFromSelectOutputReferenceOrExpression(sortKey, context, "ORDER BY");
        sortSymbolValidator.process(sortSymbol, new SortSymbolValidator.SortContext(context.table));
        return sortSymbol;
    }

    @Override
    protected Symbol visitQuery(Query node, SelectAnalysis context) {
        context.query(node);
        return super.visitQuery(node, context);
    }

    @Override
    public Analysis newAnalysis(ParameterContext parameterContext) {
        return new SelectAnalysis(referenceInfos, functions, parameterContext, globalReferenceResolver);
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

    /**
     * validate that sortSymbols don't contain partition by columns
     */
    static class SortSymbolValidator extends SymbolVisitor<SortSymbolValidator.SortContext, Void> {

        static class SortContext {
            private final TableInfo tableInfo;
            private boolean inFunction;
            public SortContext(TableInfo tableInfo) {
                this.tableInfo = tableInfo;
                this.inFunction = false;
            }
        }

        @Override
        public Void visitFunction(Function symbol, SortContext context) {
            try {
                if (context.inFunction == false
                        && !DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                    throw new UnsupportedOperationException(
                            String.format(Locale.ENGLISH,
                                    "Cannot ORDER BY '%s': invalid return type '%s'.",
                                    SymbolFormatter.format(symbol),
                                    symbol.valueType())
                    );
                }

                if (symbol.info().type() == FunctionInfo.Type.PREDICATE) {
                    throw new UnsupportedOperationException(String.format(
                            "%s predicate cannot be used in an ORDER BY clause", symbol.info().ident().name()));
                }

                context.inFunction = true;
                for (Symbol arg : symbol.arguments()) {
                    process(arg, context);
                }
            } finally {
                context.inFunction = false;
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, SortContext context) {
            if (context.tableInfo.partitionedBy().contains(symbol.info().ident().columnIdent())) {
                throw new UnsupportedOperationException(
                        SymbolFormatter.format(
                                "cannot use partitioned column %s in ORDER BY clause",
                                symbol));
            }
            // if we are in a function, we do not need to check the data type.
            // the function will do that for us.
            if (!context.inFunction && !DataTypes.PRIMITIVE_TYPES.contains(symbol.info().type())) {
                throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH,
                                "Cannot ORDER BY '%s': invalid data type '%s'.",
                                SymbolFormatter.format(symbol),
                                symbol.valueType())
                );
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
            }
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, SortContext context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot order by \"%s\". The column doesn't exist.", symbol));
        }

        @Override
        public Void visitSymbol(Symbol symbol, SortContext context) {
            return null;
        }
    }

    static class GroupBySymbolValidator extends SymbolVisitor<Void, Void> {

        @Override
        public Void visitDynamicReference(DynamicReference symbol, Void context) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("unknown column '%s' not allowed in GROUP BY", symbol));
        }

        @Override
        public Void visitReference(Reference symbol, Void context) {
            if (!DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': invalid data type '%s'",
                                SymbolFormatter.format(symbol),
                                symbol.valueType()));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': grouping on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': grouping on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
            }
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, Void context) {
            switch (symbol.info().type()) {
                case SCALAR:
                    break;
                case AGGREGATE:
                    throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
                case PREDICATE:
                    throw new UnsupportedOperationException(String.format(
                            "%s predicate cannot be used in a GROUP BY clause", symbol.info().ident().name()));
                default:
                    throw new UnsupportedOperationException(
                            String.format("FunctionInfo.Type %s not handled", symbol.info().type()));
            }
            return null;
         }

        @Override
        protected Void visitSymbol(Symbol symbol, Void context) {
            throw new UnsupportedOperationException(
                    String.format("Cannot GROUP BY for '%s'", SymbolFormatter.format(symbol))
            );
        }
    }

    static class HavingSymbolValidator extends SymbolVisitor<HavingSymbolValidator.HavingContext, Void> {
        static class HavingContext {
            private final Optional<List<Symbol>> groupBySymbols;

            private boolean insideAggregation = false;

            public HavingContext(@Nullable List<Symbol> groupBySymbols) {
                this.groupBySymbols = Optional.fromNullable(groupBySymbols);
            }
        }

        @Override
        public Void visitReference(Reference symbol, HavingContext context) {
            if (!context.insideAggregation && (!context.groupBySymbols.isPresent() || !context.groupBySymbols.get().contains(symbol)) ) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("Cannot use reference %s outside of an Aggregation in HAVING clause. Only GROUP BY keys allowed here.", symbol));
            }
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, HavingContext context) {
            if (symbol.info().type().equals(FunctionInfo.Type.AGGREGATE)) {
                context.insideAggregation = true;
            }
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            context.insideAggregation = false;
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, HavingContext context) {
            return null;
        }
    }

    static class SelectSymbolValidator extends SymbolVisitor<SelectSymbolValidator.SelectContext, Void> {

        static class SelectContext {
            private boolean selectFromFieldCache;

            public SelectContext(boolean selectFromFieldCache) {
                this.selectFromFieldCache = selectFromFieldCache;
            }
        }


        @Override
        public Void visitReference(Reference symbol, SelectContext context) {
            if (context.selectFromFieldCache) {
                if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                    throw new IllegalArgumentException(
                            String.format("Cannot select analyzed column '%s' " +
                                            "within grouping or aggregations",
                                    SymbolFormatter.format(symbol)));
                } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                    throw new IllegalArgumentException(
                            String.format("Cannot select non-indexed column '%s' " +
                                            "within grouping or aggregations",
                                    SymbolFormatter.format(symbol)));
                }
            }
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, SelectContext context) {
            switch (symbol.info().type()) {
                case SCALAR:
                    break;
                case AGGREGATE:
                    context.selectFromFieldCache = true;
                    break;
                case PREDICATE:
                    throw new UnsupportedOperationException(String.format(
                            "%s predicate cannot be selected", symbol.info().ident().name()));
                default:
                    throw new UnsupportedOperationException(String.format(
                            "FunctionInfo.Type %s not handled", symbol.info().type()));
            }
            for (Symbol arg : symbol.arguments()) {
                process(arg, context);
            }
            return null;
        }

        @Override
        public Void visitSymbol(Symbol symbol, SelectContext context) {
            return null;
        }

    }

}
