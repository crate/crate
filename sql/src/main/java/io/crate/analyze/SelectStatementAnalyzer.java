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
import com.google.common.base.Preconditions;
import io.crate.exceptions.SQLParseException;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SelectStatementAnalyzer extends DataStatementAnalyzer<SelectAnalysis> {

    private final static AggregationSearcher aggregationSearcher = new AggregationSearcher();
    private final static SortSymbolValidator sortSymbolValidator = new SortSymbolValidator();

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

    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, SelectAnalysis context) {
        // only check for alias if we only have one name part
        if (node.getName().getParts().size() == 1) {
            Symbol symbol = context.symbolFromAlias(node.getSuffix().getSuffix());
            if (symbol != null) {
                return symbol;
            }
        }
        ReferenceIdent ident = context.getReference(node.getName());
        return context.allocateReference(ident);
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

        if (node.getLimit().isPresent()) {
            context.limit(extractIntegerFromNode(node.getLimit().get(), "limit", context));
        }
        if (node.getOffset().isPresent()) {
            context.offset(extractIntegerFromNode(node.getOffset().get(), "offset", context));
        }

        if (node.getWhere().isPresent()) {
            processWhereClause(node.getWhere().get(), context);
        }

        process(node.getSelect(), context);

        if (!node.getGroupBy().isEmpty()) {
            analyzeGroupBy(node.getGroupBy(), context);
        }

        if (!node.getGroupBy().isEmpty() || context.hasAggregates()) {
            ensureNonAggregatesInGroupBy(context);
        }

        if (node.getSelect().isDistinct() && node.getGroupBy().isEmpty()) {
            rewriteGlobalDistinct(context);
        }

        Preconditions.checkArgument(!node.getHaving().isPresent(), "having clause is not yet supported");

        if (node.getOrderBy().size() > 0) {
            addSorting(node.getOrderBy(), context);
        }
        return null;
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

    private Symbol ordinalOutputReference(List<Symbol> outputSymbols, Symbol symbol, String clauseName) {
        Symbol s = symbol;
        if (s.symbolType() == SymbolType.PARAMETER) {
            s = Literal.toLiteral(s, DataTypes.LONG);
        }
        int idx;
        if (s.symbolType() == SymbolType.LITERAL && ((Literal)s).valueType().equals(DataTypes.LONG)) {
            idx = ((Number)((Literal)s).value()).intValue() - 1;
            if (idx < 0) {
                throw new IllegalArgumentException(String.format(
                        "%s position %s is not in select list", clauseName, idx + 1));
            }
        } else {
            idx = outputSymbols.indexOf(s);
        }

        if (idx >= 0) {
            try {
                return outputSymbols.get(idx);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(String.format(
                            "%s position %s is not in select list", clauseName, idx + 1));
            }
        }
        return null;
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

    private Integer extractIntegerFromNode(Expression expression, String clauseName, SelectAnalysis context) {
        Symbol symbol = process(expression, context);
        assert symbol.symbolType().isValueSymbol(); // due to parser this must be a parameterNode or integer
        try {
            if (symbol.symbolType() == SymbolType.PARAMETER) {
                return DataTypes.INTEGER.value(((Parameter) symbol).value());
            }
            return DataTypes.INTEGER.value(((Literal) symbol).value());
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "The parameter %s that was passed to %s has an invalid type", SymbolFormatter.format(symbol), clauseName), e);
        }
    }

    private void analyzeGroupBy(List<Expression> groupByExpressions, SelectAnalysis context) {
        List<Symbol> groupBy = new ArrayList<>(groupByExpressions.size());
        for (Expression expression : groupByExpressions) {
            Symbol s = process(expression, context);
            Symbol refOutput = ordinalOutputReference(context.outputSymbols(), s, "GROUP BY");
            s = Objects.firstNonNull(refOutput, s);
            if (s.symbolType() == SymbolType.DYNAMIC_REFERENCE) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("unknown column '%s' not allowed in GROUP BY", s));
            } else if (s.symbolType() == SymbolType.FUNCTION && ((Function) s).info().isAggregate()) {
                throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
            } else if (s.symbolType() == SymbolType.REFERENCE && !DataTypes.PRIMITIVE_TYPES.contains(((Reference)s).valueType())) {
                throw new IllegalArgumentException(
                        String.format("Cannot group by '%s': invalid data type '%s'",
                                SymbolFormatter.format(s),
                                ((Reference) s).valueType()));
            }
            groupBy.add(s);
        }
        context.groupBy(groupBy);
    }

    private void ensureNonAggregatesInGroupBy(SelectAnalysis context) {
        for (Symbol symbol : context.outputSymbols()) {
            if (context.groupBy() == null || !context.groupBy().contains(symbol)) {
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
            if (((Function) s).info().isAggregate()) {
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
        Symbol sortSymbol = super.visitSortItem(node, context);
        if (sortSymbol.symbolType() == SymbolType.PARAMETER) {
            sortSymbol = Literal.fromParameter((Parameter)sortSymbol);
        }
        if (sortSymbol.symbolType() == SymbolType.LITERAL && DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(((Literal)sortSymbol).valueType())) {
            // deref
            sortSymbol = ordinalOutputReference(context.outputSymbols(), sortSymbol, "ORDER BY");
        }
        // validate sortSymbol
        sortSymbolValidator.process(sortSymbol, new SortSymbolValidator.SortContext(context.table));
        return sortSymbol;
    }

    @Override
    protected Symbol visitQuery(Query node, SelectAnalysis context) {
        context.query(node);
        return super.visitQuery(node, context);
    }

    static class AggregationSearcherContext {
        boolean found = false;
    }

    static class AggregationSearcher extends SymbolVisitor<AggregationSearcherContext, Void> {

        @Override
        public Void visitFunction(Function symbol, AggregationSearcherContext context) {
            if (symbol.info().isAggregate()) {
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
                context.inFunction = true;
                if (!DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                    throw new UnsupportedOperationException(
                            String.format(Locale.ENGLISH,
                                    "Cannot ORDER BY '%s': invalid return type '%s'.",
                                    SymbolFormatter.format(symbol),
                                    symbol.valueType())
                    );
                }
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
            }
            return null;
        }

        @Override
        public Void visitSymbol(Symbol symbol, SortContext context) {
            return null;
        }
    }
}
