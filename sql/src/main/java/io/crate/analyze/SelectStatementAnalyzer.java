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
import io.crate.analyze.where.WhereClause;
import io.crate.exceptions.SQLParseException;
import io.crate.metadata.*;
import io.crate.metadata.relation.AliasedAnalyzedRelation;
import io.crate.metadata.relation.AnalyzedQuerySpecification;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.relation.TableRelation;
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
        for (ReferenceInfo referenceInfo : context.table().columns()) {
            // ignore NOT_SUPPORTED columns
            if (referenceInfo.type() != DataTypes.NOT_SUPPORTED) {
                Reference ref = new Reference(referenceInfo);
                context.allocationContext().allocatedReferences.put(ref.info(), ref);
                context.outputSymbols().add(ref);
                context.addAlias(referenceInfo.ident().columnIdent().name(), ref);
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

        return context.allocationContext().resolveReference(node.getName());
    }

    protected Symbol visitQuerySpecification(QuerySpecification node, SelectAnalysis context) {
        List<Relation> from = node.getFrom();
        if (from == null) {
            throw new SQLParseException("FROM clause is missing in SELECT statement");
        }
        if (from.size() != 1) {
            throw new SQLParseException(
                    "Only exactly one table is allowed in the from clause, got: " + from.size());
        }

        // TODO: remove 1 table limitation and build joins...
//        List<AnalyzedRelation> analyzedRelations = new ArrayList<>(from.size());
//        for (Relation relation : from) {
//            analyzedRelations.add(convert(relation));
//        }
//        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
//                functions, referenceInfos, analyzedRelations);
//
//        WhereClause whereClause = getWhereClause(node.getWhere());
//        AnalyzedRelation sourceRelation = JoinDetector.buildRelation(analyzedRelations, whereClause);

        RelationSymbol relationSymbol = (RelationSymbol) process(from.get(0), context);
        AnalyzedRelation analyzedRelation = relationSymbol.relation();

        // TODO: remove context.table
        if (analyzedRelation instanceof TableRelation) {
            context.table = ((TableRelation) analyzedRelation).tableInfo();
        } else if (analyzedRelation instanceof AliasedAnalyzedRelation) {
            AnalyzedRelation child = analyzedRelation.children().get(0);
            if (child instanceof TableRelation) {
                context.table = ((TableRelation) child).tableInfo();
            }
        }
        Integer limit = intFromOptionalExpression(node.getLimit(), context.parameters());
        Integer offset = intFromOptionalExpression(node.getOffset(), context.parameters());

        if (node.getWhere().isPresent()) {
            Symbol query = process(node.getWhere().get(), context);
            analyzedRelation.whereClause(new WhereClause(context.normalizer.normalize(query)));
        } else {
            analyzedRelation.whereClause(WhereClause.MATCH_ALL);
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

        Symbol having = null;
        if (node.getHaving().isPresent()) {
            if (node.getGroupBy().isEmpty() && !context.hasAggregates()) {
                throw new IllegalArgumentException("HAVING clause can only be used in GROUP BY or global aggregate queries");
            }
            having = generateHaving(node.getHaving().get(), context);
        }

        if (node.getOrderBy().size() > 0) {
            addSorting(node.getOrderBy(), context);
        }

        // TODO: re-write aliasedRelations to non-aliased because alias isn't required anymore
        // at this point as all references have been resolved

        // TODO: this is just a temporary solution to make the relation usable in the Planner and other places
        // the analyzer will be changed to create the relation directly
        // instead of setting all attributes onto the context..
        AnalyzedQuerySpecification relation = new AnalyzedQuerySpecification(
                context.outputSymbols(),
                analyzedRelation,
                context.groupBy(),
                having,
                context.sortSymbols(),
                context.reverseFlags(),
                context.nullsFirst(),
                limit,
                offset
        );
        context.querySpecification(relation);
        return new RelationSymbol(relation);
    }

    private Symbol generateHaving(Expression expression, SelectAnalysis context) {
        Symbol havingQuery = process(expression, context);

        // validate having symbols
        HavingSymbolValidator.HavingContext havingContext = new HavingSymbolValidator.HavingContext(context.groupBy());
        havingSymbolValidator.process(havingQuery, havingContext);

        return context.normalizer.normalize(havingQuery);
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

    private void analyzeGroupBy(List<Expression> groupByExpressions, SelectAnalysis context) {
        List<Symbol> groupBy = new ArrayList<>(groupByExpressions.size());
        for (Expression expression : groupByExpressions) {
            Symbol s = process(expression, context);
            Symbol deRef = ordinalOutputReference(context.outputSymbols(), s, "GROUP BY");
            s = Objects.firstNonNull(deRef, s);
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
        Symbol sortSymbol = super.visitSortItem(node, context);
        if (sortSymbol.symbolType() == SymbolType.PARAMETER) {
            sortSymbol = Literal.fromParameter((Parameter)sortSymbol);
        }
        if (sortSymbol.symbolType() == SymbolType.LITERAL && DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(((Literal)sortSymbol).valueType())) {
            // de-ref
            sortSymbol = ordinalOutputReference(context.outputSymbols(), sortSymbol, "ORDER BY");
        }
        // validate sortSymbol
        sortSymbolValidator.process(sortSymbol, new SortSymbolValidator.SortContext(context.table));
        return sortSymbol;
    }

    @Override
    protected Symbol visitQuery(Query node, SelectAnalysis context) {
        return super.visitQuery(node, context);
    }

    @Override
    protected DataTypeSymbol resolveSubscriptSymbol(SubscriptContext subscriptContext, SelectAnalysis context) {
        DataTypeSymbol dataTypeSymbol = null;
        // resolve possible alias
        if (subscriptContext.parts().size() == 0 && subscriptContext.qName() != null) {
            Symbol symbol = context.symbolFromAlias(subscriptContext.qName().getSuffix());
            if (symbol != null) {
                dataTypeSymbol = (DataTypeSymbol) symbol;
            }
        }

        if (dataTypeSymbol == null) {
            dataTypeSymbol = super.resolveSubscriptSymbol(subscriptContext, context);
        }
        return dataTypeSymbol;
    }

    @Override
    public Analysis newAnalysis(Analyzer.ParameterContext parameterContext) {
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
