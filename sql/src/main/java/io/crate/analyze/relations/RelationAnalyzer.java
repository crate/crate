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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.*;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.analyze.validator.GroupBySymbolValidator;
import io.crate.analyze.validator.HavingSymbolValidator;
import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.RelationUnknownException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.planner.consumer.OrderByWithAggregationValidator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RelationAnalyzer extends DefaultTraversalVisitor<AnalyzedRelation, StatementAnalysisContext> {

    private final ClusterService clusterService;
    private final Functions functions;
    private final Schemas schemas;
    private static final List<Relation> SYS_CLUSTER_SOURCE = ImmutableList.<Relation>of(
        new Table(new QualifiedName(
            ImmutableList.of(SysClusterTableInfo.IDENT.schema(), SysClusterTableInfo.IDENT.name()))
        )
    );

    public RelationAnalyzer(ClusterService clusterService, Functions functions, Schemas schemas) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.schemas = schemas;
    }

    public AnalyzedRelation analyze(Node node,
                                    StatementAnalysisContext statementContext,
                                    TransactionContext transactionContext) {
        AnalyzedRelation relation = process(node, statementContext);
        return RelationNormalizer.normalize(relation, functions, transactionContext);
    }

    public AnalyzedRelation analyzeUnbound(Query query, SessionContext sessionContext, ParamTypeHints paramTypeHints) {
        return process(query, new StatementAnalysisContext(
            sessionContext,
            paramTypeHints,
            functions,
            Operation.READ));
    }

    public AnalyzedRelation analyze(Node node, Analysis analysis) {
        return analyze(
            node,
            new StatementAnalysisContext(
                analysis.sessionContext(),
                analysis.parameterContext(),
                functions,
                Operation.READ
            ),
            analysis.transactionContext()
        );
    }

    @Override
    protected AnalyzedRelation visitQuery(Query node, StatementAnalysisContext context) {
        // In case of Set Operation
        if (!node.getOrderBy().isEmpty() || node.getLimit().isPresent() || node.getOffset().isPresent()) {
            TwoRelationsUnion twoRelationsUnion = (TwoRelationsUnion) process(node.getQueryBody(), context);

            // Use first relation of the top level Union to process expressions of the "root" Query node
            context.startRelation();
            RelationAnalysisContext analysisContext = context.currentRelationContext();
            analysisContext.addSourceRelation(twoRelationsUnion.first().getQualifiedName().toString(), twoRelationsUnion.first());
            SelectAnalysis selectAnalysis = new SelectAnalysis(analysisContext);
            for (Field field : twoRelationsUnion.fields()) {
                selectAnalysis.add(field.path(), field);
            }
            context.endRelation();

            QuerySpec querySpec = twoRelationsUnion.querySpec();
            if (node.getLimit().isPresent()) {
                querySpec.limit(Optional.fromNullable(analysisContext.expressionAnalyzer().convert(
                    node.getLimit().get(),
                    analysisContext.expressionAnalysisContext())));
            }
            if (node.getOffset().isPresent()) {
                querySpec.offset(Optional.fromNullable(analysisContext.expressionAnalyzer().convert(
                    node.getOffset().get(),
                    analysisContext.expressionAnalysisContext())));
            }
            querySpec.orderBy(analyzeOrderBy(selectAnalysis, node.getOrderBy(), analysisContext, false, false));

            return twoRelationsUnion;
        }
        return process(node.getQueryBody(), context);
    }

    @Override
    protected AnalyzedRelation visitUnion(Union node, StatementAnalysisContext context) {
        if (node.isDistinct()) {
            throw new UnsupportedOperationException("UNION [DISTINCT] is not supported");
        } else {
            // Parser builds a tree of union pairs so every pair has always 2 relations
            assert node.getRelations().size() == 2 : "Each Union can have only two relations";
            QueriedRelation first = (QueriedRelation) process(node.getRelations().get(0), context);
            QueriedRelation second = (QueriedRelation) process(node.getRelations().get(1), context);
            TwoRelationsUnion twoRelationsUnion = new TwoRelationsUnion(first, second, node.isDistinct());

            // Check number of outputs
            if (first.querySpec().outputs().size() != second.querySpec().outputs().size()) {
                throw new UnsupportedOperationException("Number of output columns must be the same for all parts of a UNION");
            }
            // Try to cast outputs
            List<DataType> dataTypesFromLeft = new ArrayList<>();
            for (Symbol outputSymbol : first.querySpec().outputs()) {
                dataTypesFromLeft.add(outputSymbol.valueType());
            }
            if (second.querySpec().castOutputs(dataTypesFromLeft.iterator()) >= 0) {
                throw new UnsupportedOperationException("Corresponding output columns must be compatible for all parts of a UNION");
            }
            twoRelationsUnion.querySpec().outputs(first.querySpec().outputs());

            return twoRelationsUnion;
        }
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
                try {
                    joinCondition = relationContext.expressionAnalyzer().convert(
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
        ExpressionAnalyzer expressionAnalyzer = context.expressionAnalyzer();
        ExpressionAnalysisContext expressionAnalysisContext = context.expressionAnalysisContext();
        Symbol querySymbol = expressionAnalyzer.generateQuerySymbol(node.getWhere(), expressionAnalysisContext);
        WhereClause whereClause = new WhereClause(querySymbol);

        SelectAnalysis selectAnalysis = SelectAnalyzer.analyzeSelect(node.getSelect(), context);

        List<Symbol> groupBy = analyzeGroupBy(selectAnalysis, node.getGroupBy(), context);

        if (!node.getGroupBy().isEmpty() || expressionAnalysisContext.hasAggregates) {
            ensureNonAggregatesInGroupBy(selectAnalysis.outputSymbols(), groupBy);
        }
        boolean isDistinct = false;
        if (node.getSelect().isDistinct() && groupBy.isEmpty()) {
            groupBy = rewriteGlobalDistinct(selectAnalysis.outputSymbols());
            isDistinct = true;
        }
        if (groupBy != null && groupBy.isEmpty()) {
            groupBy = null;
        }
        QuerySpec querySpec = new QuerySpec()
            .orderBy(analyzeOrderBy(selectAnalysis, node.getOrderBy(), context,
                expressionAnalysisContext.hasAggregates || groupBy != null, isDistinct))
            .having(analyzeHaving(
                node.getHaving(),
                groupBy,
                expressionAnalyzer,
                context.expressionAnalysisContext()))
            .limit(optionalLongSymbol(node.getLimit(), context))
            .offset(optionalLongSymbol(node.getOffset(), context))
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
            // TODO: implement multi table selects
            // once this is used .normalize should for this class needs to be handled here too
            relation = new MultiSourceSelect(
                context.sources(),
                selectAnalysis.outputSymbols(),
                selectAnalysis.outputNames(),
                querySpec,
                context.joinPairs()
            );
        }

        statementContext.endRelation();
        return relation;
    }

    private Optional<Symbol> optionalLongSymbol(Optional<Expression> optExpression, RelationAnalysisContext relCtx) {
        if (optExpression.isPresent()) {
            Symbol symbol = relCtx.expressionAnalyzer().convert(optExpression.get(), relCtx.expressionAnalysisContext());
            return Optional.of(ExpressionAnalyzer.castIfNeededOrFail(symbol, DataTypes.LONG));
        }
        return Optional.absent();
    }

    @Nullable
    private List<Symbol> rewriteGlobalDistinct(List<Symbol> outputSymbols) {
        List<Symbol> groupBy = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            if (!Aggregations.containsAggregation(symbol)) {
                GroupBySymbolValidator.validate(symbol);
                groupBy.add(symbol);
            }
        }
        return groupBy;
    }

    private void ensureNonAggregatesInGroupBy(List<Symbol> outputSymbols, List<Symbol> groupBy) throws IllegalArgumentException {
        for (Symbol output : outputSymbols) {
            if (groupBy == null || !groupBy.contains(output)) {
                if (!Aggregations.containsAggregation(output)) {
                    throw new IllegalArgumentException(
                        SymbolFormatter.format("column '%s' must appear in the GROUP BY clause " +
                                               "or be used in an aggregation function", output));
                }
            }
        }
    }

    @Nullable
    private OrderBy analyzeOrderBy(SelectAnalysis selectAnalysis,
                                   List<SortItem> orderBy,
                                   RelationAnalysisContext context,
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
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(sortKey, selectAnalysis, "ORDER BY", context);
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

    private List<Symbol> analyzeGroupBy(SelectAnalysis selectAnalysis, List<Expression> groupBy,
                                        RelationAnalysisContext context) {
        List<Symbol> groupBySymbols = new ArrayList<>(groupBy.size());
        for (Expression expression : groupBy) {
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(
                expression, selectAnalysis, "GROUP BY", context);
            GroupBySymbolValidator.validate(symbol);
            groupBySymbols.add(symbol);
        }
        return groupBySymbols;
    }

    private HavingClause analyzeHaving(Optional<Expression> having,
                                       List<Symbol> groupBy,
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
    private Symbol symbolFromSelectOutputReferenceOrExpression(Expression expression,
                                                               SelectAnalysis selectAnalysis,
                                                               String clause,
                                                               RelationAnalysisContext context) {
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
        symbol = context.expressionAnalyzer().convert(expression, context.expressionAnalysisContext());
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

    private Symbol ordinalOutputReference(List<Symbol> outputSymbols, Literal longLiteral, String clauseName) {
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
        TableInfo tableInfo = schemas.getTableInfo(TableIdent.of(node, context.sessionContext().defaultSchema()));
        Operation.blockedRaiseException(tableInfo, context.currentOperation());
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
        });

        List<Symbol> arguments = new ArrayList<>(node.arguments().size());
        for (Expression expression : node.arguments()) {
            Symbol symbol = expressionAnalyzer.convert(expression, context.expressionAnalysisContext());
            arguments.add(symbol);
        }
        TableFunctionImplementation tableFunction = functions.getTableFunctionSafe(node.name());
        TableInfo tableInfo = tableFunction.createTableInfo(clusterService, Symbols.extractTypes(arguments));
        Operation.blockedRaiseException(tableInfo, statementContext.currentOperation());
        TableRelation tableRelation = new TableFunctionRelation(tableInfo, node.name(), arguments);
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
