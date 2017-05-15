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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.*;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.analyze.validator.GroupBySymbolValidator;
import io.crate.analyze.validator.HavingSymbolValidator;
import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.RelationUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.planner.consumer.OrderByWithAggregationValidator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.*;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.set.Sets;

import javax.annotation.Nullable;
import java.util.*;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RelationAnalyzer extends DefaultTraversalVisitor<AnalyzedRelation, StatementAnalysisContext> {

    private final ClusterService clusterService;
    private final Functions functions;
    private final Schemas schemas;
    private final RelationNormalizer relationNormalizer;

    private static final List<Relation> SYS_CLUSTER_SOURCE = ImmutableList.<Relation>of(
        new Table(new QualifiedName(
            ImmutableList.of(SysClusterTableInfo.IDENT.schema(), SysClusterTableInfo.IDENT.name()))
        )
    );

    public RelationAnalyzer(ClusterService clusterService, Functions functions, Schemas schemas) {
        relationNormalizer = new RelationNormalizer(functions);
        this.clusterService = clusterService;
        this.functions = functions;
        this.schemas = schemas;
    }

    public AnalyzedRelation analyze(Node node, StatementAnalysisContext statementContext) {
        AnalyzedRelation relation = process(node, statementContext);
        relation = SubselectRewriter.rewrite(relation);
        return relationNormalizer.normalize(relation, statementContext.transactionContext());
    }

    public AnalyzedRelation analyzeUnbound(Query query, SessionContext sessionContext, ParamTypeHints paramTypeHints) {
        return process(query, new StatementAnalysisContext(sessionContext, paramTypeHints, Operation.READ, null));
    }

    public AnalyzedRelation analyze(Node node, Analysis analysis) {
        return analyze(
            node,
            new StatementAnalysisContext(
                analysis.sessionContext(),
                analysis.parameterContext(),
                Operation.READ,
                analysis.transactionContext())
        );
    }

    @Override
    protected AnalyzedRelation visitQuery(Query node, StatementAnalysisContext context) {
        return process(node.getQueryBody(), context);
    }

    @Override
    protected AnalyzedRelation visitUnion(Union node, StatementAnalysisContext context) {
        throw new UnsupportedFeatureException("UNION is not supported");
    }

    @Override
    protected AnalyzedRelation visitIntersect(Intersect node, StatementAnalysisContext context) {
        throw new UnsupportedFeatureException("INTERSECT is not supported");
    }

    @Override
    protected AnalyzedRelation visitExcept(Except node, StatementAnalysisContext context) {
        throw new UnsupportedFeatureException("EXCEPT is not supported");
    }

    @Override
    protected AnalyzedRelation visitJoin(Join node, StatementAnalysisContext statementContext) {
        process(node.getLeft(), statementContext);
        process(node.getRight(), statementContext);

        RelationAnalysisContext relationContext = statementContext.currentRelationContext();
        Optional<JoinCriteria> optCriteria = node.getCriteria();
        Symbol joinCondition = null;
        if (optCriteria.isPresent()) {
            JoinCriteria joinCriteria = optCriteria.get();
            if (joinCriteria instanceof JoinOn) {
                ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                    functions,
                    statementContext.sessionContext(),
                    statementContext.convertParamFunction(),
                    new FullQualifedNameFieldProvider(relationContext.sources()),
                    new SubqueryAnalyzer(this, statementContext));
                try {
                    joinCondition = expressionAnalyzer.convert(
                        ((JoinOn) joinCriteria).getExpression(), relationContext.expressionAnalysisContext());
                } catch (RelationUnknownException e) {
                    throw new ValidationException(String.format(Locale.ENGLISH,
                        "missing FROM-clause entry for relation '%s'", e.qualifiedName()));
                }
            } else {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "join criteria %s not supported",
                    joinCriteria.getClass().getSimpleName()));
            }
        }

        relationContext.addJoinType(JoinType.values()[node.getType().ordinal()], joinCondition);
        return null;
    }

    @Override
    protected AnalyzedRelation visitQuerySpecification(QuerySpecification node, StatementAnalysisContext statementContext) {
        List<Relation> from = node.getFrom() != null ? node.getFrom() : SYS_CLUSTER_SOURCE;

        statementContext.startRelation();

        for (Relation relation : from) {
            process(relation, statementContext);
        }

        RelationAnalysisContext context = statementContext.currentRelationContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            statementContext.sessionContext(),
            statementContext.convertParamFunction(),
            new FullQualifedNameFieldProvider(context.sources()),
            new SubqueryAnalyzer(this, statementContext));
        ExpressionAnalysisContext expressionAnalysisContext = context.expressionAnalysisContext();
        Symbol querySymbol = expressionAnalyzer.generateQuerySymbol(node.getWhere(), expressionAnalysisContext);
        WhereClause whereClause = new WhereClause(querySymbol);

        SelectAnalysis selectAnalysis = SelectAnalyzer.analyzeSelect(
            node.getSelect(), context.sources(), expressionAnalyzer, expressionAnalysisContext);

        List<Symbol> groupBy = analyzeGroupBy(
            selectAnalysis,
            node.getGroupBy(),
            expressionAnalyzer,
            expressionAnalysisContext);

        if (!node.getGroupBy().isEmpty() || expressionAnalysisContext.hasAggregates) {
            ensureNonAggregatesInGroupBy(selectAnalysis.outputSymbols(), selectAnalysis.outputNames(), groupBy);
        }

        boolean distinctProcessed = false;
        boolean isDistinct = node.getSelect().isDistinct();
        if (node.getSelect().isDistinct()) {
            List<Symbol> newGroupBy = rewriteGlobalDistinct(selectAnalysis.outputSymbols());
            if (groupBy.isEmpty() || Sets.newHashSet(newGroupBy).equals(Sets.newHashSet(groupBy))) {
                distinctProcessed = true;
            }
            if (groupBy.isEmpty()) {
                groupBy = newGroupBy;
            }
        }
        if (groupBy != null && groupBy.isEmpty()) {
            groupBy = null;
        }
        QuerySpec querySpec = new QuerySpec()
            .orderBy(analyzeOrderBy(
                selectAnalysis,
                node.getOrderBy(),
                expressionAnalyzer,
                expressionAnalysisContext,
                expressionAnalysisContext.hasAggregates || groupBy != null, isDistinct))
            .having(analyzeHaving(
                node.getHaving(),
                groupBy,
                expressionAnalyzer,
                context.expressionAnalysisContext()))
            .limit(optionalLongSymbol(node.getLimit(), expressionAnalyzer, expressionAnalysisContext))
            .offset(optionalLongSymbol(node.getOffset(), expressionAnalyzer, expressionAnalysisContext))
            .outputs(selectAnalysis.outputSymbols())
            .where(whereClause)
            .groupBy(groupBy)
            .hasAggregates(expressionAnalysisContext.hasAggregates);

        QueriedRelation relation;
        if (context.sources().size() == 1) {
            AnalyzedRelation source = Iterables.getOnlyElement(context.sources().values());
            if (source instanceof DocTableRelation) {
                relation = new QueriedDocTable((DocTableRelation) source, selectAnalysis.outputNames(), querySpec);
            } else if (source instanceof TableRelation) {
                relation = new QueriedTable((TableRelation) source, selectAnalysis.outputNames(), querySpec);
            } else {
                assert source instanceof QueriedRelation : "expecting relation to be an instance of QueriedRelation";
                relation = new QueriedSelectRelation((QueriedRelation) source, selectAnalysis.outputNames(), querySpec);
            }
        } else {
            relation = new MultiSourceSelect(
                context.sources(),
                selectAnalysis.outputNames(),
                querySpec,
                context.joinPairs()
            );
        }

        relation = processDistinct(distinctProcessed, isDistinct, querySpec, relation);
        statementContext.endRelation();
        return relation;
    }


    /**
     * If DISTINCT is not processed "wrap" the relation with an external QueriedSelectRelation
     * which transforms the distinct select symbols into GROUP BY symbols.
     */
    private static QueriedRelation processDistinct(boolean distinctProcessed,
                                                   boolean isDistinct,
                                                   QuerySpec querySpec,
                                                   QueriedRelation relation) {
        if (!isDistinct || distinctProcessed) {
            return relation;
        }

        // Rewrite ORDER BY so it can be applied to the "wrapper" QueriedSelectRelation
        // Since ORDER BY symbols must be subset of the select SYMBOLS we use the index
        // of the ORDER BY symbol in the list of select symbols and we use this index to
        // rewrite the symbol as the corresponding field of the relation
        OrderBy newOrderBy = null;
        if (relation.querySpec().orderBy().isPresent()) {
            OrderBy oldOrderBy = relation.querySpec().orderBy().get();
            List<Symbol> orderBySymbols = new ArrayList<>();
            for (Symbol symbol : oldOrderBy.orderBySymbols()) {
                int idx = querySpec.outputs().indexOf(symbol);
                orderBySymbols.add(relation.fields().get(idx));

            }
            newOrderBy = new OrderBy(orderBySymbols, oldOrderBy.reverseFlags(), oldOrderBy.nullsFirst());
            relation.querySpec().orderBy(null);
        }

        // LIMIT & OFFSET from the inner query must be applied after
        // the outer GROUP BY which implements the DISTINCT
        Optional<Symbol> limit = querySpec.limit();
        querySpec.limit(Optional.empty());
        Optional<Symbol> offset = querySpec.offset();
        querySpec.offset(Optional.empty());

        List<Symbol> newQspecSymbols = new ArrayList<>(relation.fields());
        QuerySpec newQuerySpec = new QuerySpec()
            .outputs(newQspecSymbols)
            .groupBy(newQspecSymbols)
            .orderBy(newOrderBy)
            .limit(limit)
            .offset(offset);
        relation = new QueriedSelectRelation(relation, relation.fields(), newQuerySpec);
        return relation;
    }

    private static Optional<Symbol> optionalLongSymbol(Optional<Expression> optExpression,
                                                       ExpressionAnalyzer expressionAnalyzer,
                                                       ExpressionAnalysisContext expressionAnalysisContext) {
        if (optExpression.isPresent()) {
            Symbol symbol = expressionAnalyzer.convert(optExpression.get(), expressionAnalysisContext);
            return Optional.of(ExpressionAnalyzer.castIfNeededOrFail(symbol, DataTypes.LONG));
        }
        return Optional.empty();
    }

    private static List<Symbol> rewriteGlobalDistinct(List<Symbol> outputSymbols) {
        List<Symbol> groupBy = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            if (!Aggregations.containsAggregation(symbol)) {
                GroupBySymbolValidator.validate(symbol);
                groupBy.add(symbol);
            }
        }
        return groupBy;
    }

    private static void ensureNonAggregatesInGroupBy(List<Symbol> outputSymbols,
                                                     List<Path> outputNames,
                                                     List<Symbol> groupBy) throws IllegalArgumentException {
        for (int i = 0; i < outputSymbols.size(); i++) {
            Symbol output = outputSymbols.get(i);
            if (groupBy == null || !groupBy.contains(output)) {
                if (!Aggregations.containsAggregation(output)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "column '%s' must appear in the GROUP BY clause " +
                                      "or be used in an aggregation function", outputNames.get(i).outputName()));
                }
            }
        }
    }

    @Nullable
    private static OrderBy analyzeOrderBy(SelectAnalysis selectAnalysis,
                                          List<SortItem> orderBy,
                                          ExpressionAnalyzer expressionAnalyzer,
                                          ExpressionAnalysisContext expressionAnalysisContext,
                                          boolean hasAggregatesOrGrouping,
                                          boolean isDistinct) {
        int size = orderBy.size();
        if (size == 0) {
            return null;
        }
        List<Symbol> symbols = new ArrayList<>(size);
        boolean[] reverseFlags = new boolean[size];
        Boolean[] nullsFirst = new Boolean[size];

        for (int i = 0; i < size; i++) {
            SortItem sortItem = orderBy.get(i);
            Expression sortKey = sortItem.getSortKey();
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(
                sortKey, selectAnalysis, "ORDER BY", expressionAnalyzer, expressionAnalysisContext);
            SemanticSortValidator.validate(symbol);
            if (hasAggregatesOrGrouping) {
                OrderByWithAggregationValidator.validate(symbol, selectAnalysis.outputSymbols(), isDistinct);
            }

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

    private List<Symbol> analyzeGroupBy(SelectAnalysis selectAnalysis,
                                        List<Expression> groupBy,
                                        ExpressionAnalyzer expressionAnalyzer,
                                        ExpressionAnalysisContext expressionAnalysisContext) {
        List<Symbol> groupBySymbols = new ArrayList<>(groupBy.size());
        for (Expression expression : groupBy) {
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(
                expression, selectAnalysis, "GROUP BY", expressionAnalyzer, expressionAnalysisContext);
            GroupBySymbolValidator.validate(symbol);
            groupBySymbols.add(symbol);
        }
        return groupBySymbols;
    }

    private HavingClause analyzeHaving(Optional<Expression> having,
                                       @Nullable List<Symbol> groupBy,
                                       ExpressionAnalyzer expressionAnalyzer,
                                       ExpressionAnalysisContext expressionAnalysisContext) {
        if (having.isPresent()) {
            if (!expressionAnalysisContext.hasAggregates && (groupBy == null || groupBy.isEmpty())) {
                throw new IllegalArgumentException("HAVING clause can only be used in GROUP BY or global aggregate queries");
            }
            Symbol symbol = expressionAnalyzer.convert(having.get(), expressionAnalysisContext);
            HavingSymbolValidator.validate(symbol, groupBy);
            return new HavingClause(symbol);
        }
        return null;
    }


    /**
     * <h2>resolve expression by also taking alias and ordinal-reference into account</h2>
     * <p>
     * <p>
     * in group by or order by clauses it is possible to reference anything in the
     * select list by using a number or alias
     * </p>
     * <p>
     * These are allowed:
     * <pre>
     *     select name as n  ... order by n
     *     select name  ... order by 1
     *     select name ... order by other_column
     * </pre>
     */
    private static Symbol symbolFromSelectOutputReferenceOrExpression(Expression expression,
                                                                      SelectAnalysis selectAnalysis,
                                                                      String clause,
                                                                      ExpressionAnalyzer expressionAnalyzer,
                                                                      ExpressionAnalysisContext expressionAnalysisContext) {
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
                longLiteral = io.crate.analyze.symbol.Literal.convert(symbol, DataTypes.LONG);
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Cannot use %s in %s clause", SymbolPrinter.INSTANCE.printSimple(symbol), clause));
            }
            symbol = ordinalOutputReference(selectAnalysis.outputSymbols(), longLiteral, clause);
        }
        return symbol;
    }

    private static Symbol ordinalOutputReference(List<Symbol> outputSymbols, Literal longLiteral, String clauseName) {
        assert longLiteral.valueType().equals(DataTypes.LONG) : "longLiteral must have valueType long";
        int idx = ((Long) longLiteral.value()).intValue() - 1;
        if (idx < 0) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "%s position %s is not in select list", clauseName, idx + 1));
        }
        try {
            return outputSymbols.get(idx);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "%s position %s is not in select list", clauseName, idx + 1));
        }
    }

    @Nullable
    private static Symbol getOneOrAmbiguous(Multimap<String, Symbol> selectList, String key) throws AmbiguousColumnAliasException {
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
    protected AnalyzedRelation visitAliasedRelation(AliasedRelation node, StatementAnalysisContext context) {
        context.startRelation(true);
        AnalyzedRelation childRelation = process(node.getRelation(), context);
        context.endRelation();
        childRelation.setQualifiedName(new QualifiedName(node.getAlias()));
        context.currentRelationContext().addSourceRelation(node.getAlias(), childRelation);
        return childRelation;
    }

    @Override
    protected AnalyzedRelation visitTable(Table node, StatementAnalysisContext context) {
        TableInfo tableInfo = schemas.getTableInfo(TableIdent.of(node, context.sessionContext().defaultSchema()),
            context.currentOperation(), context.sessionContext().user());
        AnalyzedRelation tableRelation;
        // Dispatching of doc relations is based on the returned class of the schema information.
        if (tableInfo instanceof DocTableInfo) {
            tableRelation = new DocTableRelation((DocTableInfo) tableInfo);
        } else {
            tableRelation = new TableRelation(tableInfo);
        }
        context.currentRelationContext().addSourceRelation(
            tableInfo.ident().schema(), tableInfo.ident().name(), tableRelation);
        return tableRelation;
    }

    @Override
    public AnalyzedRelation visitTableFunction(TableFunction node, StatementAnalysisContext statementContext) {
        RelationAnalysisContext context = statementContext.currentRelationContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            statementContext.sessionContext(),
            statementContext.convertParamFunction(),
            new FieldProvider() {
                @Override
                public Symbol resolveField(QualifiedName qualifiedName, Operation operation) {
                    throw new UnsupportedOperationException("Can only resolve literals");
                }

                @Override
                public Symbol resolveField(QualifiedName qualifiedName, @Nullable List path, Operation operation) {
                    throw new UnsupportedOperationException("Can only resolve literals");
                }
            },
            null
        );

        Function function = (Function) expressionAnalyzer.convert(node.functionCall(), context.expressionAnalysisContext());
        FunctionIdent ident = function.info().ident();
        FunctionImplementation functionImplementation = functions.getQualified(ident);
        if (functionImplementation.info().type() != FunctionInfo.Type.TABLE) {
            String message = "Non table function " + ident.name() + " is not supported in from clause";
            throw new UnsupportedFeatureException(message);
        }
        TableFunctionImplementation tableFunction = (TableFunctionImplementation) functionImplementation;
        TableInfo tableInfo = tableFunction.createTableInfo(clusterService);
        Operation.blockedRaiseException(tableInfo, statementContext.currentOperation());
        TableRelation tableRelation = new TableFunctionRelation(tableInfo, tableFunction, function);
        context.addSourceRelation(node.name(), tableRelation);
        return tableRelation;
    }

    @Override
    protected AnalyzedRelation visitTableSubquery(TableSubquery node, StatementAnalysisContext context) {
        if (!context.currentRelationContext().isAliasedRelation()) {
            throw new UnsupportedOperationException("subquery in FROM must have an alias");
        }
        return super.visitTableSubquery(node, context);
    }
}
