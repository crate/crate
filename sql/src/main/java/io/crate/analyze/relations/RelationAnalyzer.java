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
import io.crate.analyze.HavingClause;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.analyze.validator.GroupBySymbolValidator;
import io.crate.analyze.validator.HavingSymbolValidator;
import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.symbol.Aggregations;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.metadata.view.ViewMetaData;
import io.crate.planner.consumer.OrderByWithAggregationValidator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Except;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.Intersect;
import io.crate.sql.tree.Join;
import io.crate.sql.tree.JoinCriteria;
import io.crate.sql.tree.JoinOn;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.Relation;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableFunction;
import io.crate.sql.tree.TableSubquery;
import io.crate.sql.tree.Union;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Singleton
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RelationAnalyzer extends DefaultTraversalVisitor<AnalyzedRelation, StatementAnalysisContext> {

    private final Functions functions;
    private final Schemas schemas;
    private final RelationNormalizer relationNormalizer;
    private final SymbolPrinter symbolPrinter;

    private static final List<Relation> EMPTY_ROW_TABLE_RELATION = ImmutableList.of(
        new TableFunction(new FunctionCall(QualifiedName.of("empty_row"), Collections.emptyList()))
    );

    @Inject
    public RelationAnalyzer(Functions functions, Schemas schemas) {
        relationNormalizer = new RelationNormalizer(functions);
        this.functions = functions;
        this.symbolPrinter = new SymbolPrinter(functions);
        this.schemas = schemas;
    }

    public AnalyzedRelation analyze(Node node, StatementAnalysisContext statementContext) {
        AnalyzedRelation relation = process(node, statementContext);
        return relationNormalizer.normalize(relation, statementContext.transactionContext());
    }

    public AnalyzedRelation analyzeUnbound(Query query,
                                           TransactionContext transactionContext,
                                           ParamTypeHints paramTypeHints) {
        return analyze(query, new StatementAnalysisContext(paramTypeHints, Operation.READ, transactionContext));
    }

    public AnalyzedRelation analyze(Node node,
                                    TransactionContext transactionContext,
                                    ParameterContext parameterContext) {
        return analyze(node, new StatementAnalysisContext(parameterContext, Operation.READ, transactionContext));
    }

    @Override
    protected AnalyzedRelation visitQuery(Query node, StatementAnalysisContext statementContext) {
        // In case of Set Operation (UNION, INTERSECT EXCEPT) or VALUES clause,
        // the `node` contains the ORDER BY and/or LIMIT and/or OFFSET and wraps the
        // actual operation (eg: UNION) which is parsed into the `queryBody` of the `node`.
        if (!node.getOrderBy().isEmpty() || node.getLimit().isPresent() || node.getOffset().isPresent()) {
            QueriedRelation childRelation = (QueriedRelation) process(node.getQueryBody(), statementContext);

            // Use child relation to process expressions of the "root" Query node
            statementContext.startRelation();
            RelationAnalysisContext relationAnalysisContext = statementContext.currentRelationContext();
            relationAnalysisContext.addSourceRelation(childRelation.getQualifiedName().toString(), childRelation);
            statementContext.endRelation();

            List<Field> childRelationFields = childRelation.fields();
            ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                functions,
                statementContext.transactionContext(),
                statementContext.convertParamFunction(),
                new FullQualifiedNameFieldProvider(
                    relationAnalysisContext.sources(),
                    relationAnalysisContext.parentSources(),
                    statementContext.transactionContext().sessionContext().searchPath().currentSchema()),
                new SubqueryAnalyzer(this, statementContext));
            ExpressionAnalysisContext expressionAnalysisContext = relationAnalysisContext.expressionAnalysisContext();
            SelectAnalysis selectAnalysis = new SelectAnalysis(
                childRelationFields.size(),
                relationAnalysisContext.sources(),
                expressionAnalyzer,
                expressionAnalysisContext);
            for (Field field : childRelationFields) {
                selectAnalysis.add(field.path(), field);
            }

            return new OrderedLimitedRelation(
                    childRelation,
                    analyzeOrderBy(
                        selectAnalysis,
                        node.getOrderBy(),
                        expressionAnalyzer,
                        expressionAnalysisContext,
                        false,
                        false),
                    longSymbolOrNull(node.getLimit(), expressionAnalyzer, expressionAnalysisContext),
                    longSymbolOrNull(node.getOffset(), expressionAnalyzer, expressionAnalysisContext));
        }
        return process(node.getQueryBody(), statementContext);
    }

    @Override
    protected AnalyzedRelation visitUnion(Union node, StatementAnalysisContext context) {
        if (node.isDistinct()) {
            throw new UnsupportedFeatureException("UNION [DISTINCT] is not supported");
        }
        QueriedRelation left = (QueriedRelation) process(node.getLeft(), context);
        QueriedRelation right = (QueriedRelation) process(node.getRight(), context);

        ensureUnionOutputsHaveTheSameSize(left, right);
        ensureUnionOutputsHaveCompatibleTypes(left, right);

        return new UnionSelect(left, right);
    }

    private static void ensureUnionOutputsHaveTheSameSize(QueriedRelation left, QueriedRelation right) {
        if (left.outputs().size() != right.outputs().size()) {
            throw new UnsupportedOperationException("Number of output columns must be the same for all parts of a UNION");
        }
    }

    private static void ensureUnionOutputsHaveCompatibleTypes(QueriedRelation left, QueriedRelation right) {
        List<Symbol> leftOutputs = left.outputs();
        List<Symbol> rightOutputs = right.outputs();
        for (int i = 0; i < leftOutputs.size(); i++) {
            if (!leftOutputs.get(i).valueType().equals(rightOutputs.get(i).valueType())) {
                throw new UnsupportedOperationException("Corresponding output columns at position: " + (i + 1) +
                                                        " must be compatible for all parts of a UNION");
            }
        }
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
                final TransactionContext transactionContext = statementContext.transactionContext();
                ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                    functions,
                    transactionContext,
                    statementContext.convertParamFunction(),
                    new FullQualifiedNameFieldProvider(
                        relationContext.sources(),
                        relationContext.parentSources(),
                        transactionContext.sessionContext().searchPath().currentSchema()),
                    new SubqueryAnalyzer(this, statementContext));
                try {
                    joinCondition = expressionAnalyzer.convert(
                        ((JoinOn) joinCriteria).getExpression(), relationContext.expressionAnalysisContext());
                } catch (RelationUnknown e) {
                    throw new RelationValidationException(e.getTableIdents(),
                        String.format(Locale.ENGLISH,
                        "missing FROM-clause entry for relation '%s'", e.getTableIdents()));
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
        List<Relation> from = node.getFrom().isEmpty() ? EMPTY_ROW_TABLE_RELATION : node.getFrom();
        RelationAnalysisContext currentRelationContext = statementContext.startRelation();

        for (Relation relation : from) {
            // different from relations have to be isolated from each other
            RelationAnalysisContext innerContext = statementContext.startRelation();
            process(relation, statementContext);
            statementContext.endRelation();
            for (Map.Entry<QualifiedName, AnalyzedRelation> entry : innerContext.sources().entrySet()) {
                currentRelationContext.addSourceRelation(entry.getKey(), entry.getValue());
            }
            for (JoinPair joinPair : innerContext.joinPairs()) {
                currentRelationContext.addJoinPair(joinPair);
            }
        }

        RelationAnalysisContext context = statementContext.currentRelationContext();
        TransactionContext transactionContext = statementContext.transactionContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            transactionContext,
            statementContext.convertParamFunction(),
            new FullQualifiedNameFieldProvider(
                context.sources(),
                context.parentSources(),
                transactionContext.sessionContext().searchPath().currentSchema()),
            new SubqueryAnalyzer(this, statementContext));
        ExpressionAnalysisContext expressionAnalysisContext = context.expressionAnalysisContext();

        SelectAnalysis selectAnalysis = SelectAnalyzer.analyzeSelect(
            node.getSelect(), context.sources(), expressionAnalyzer, expressionAnalysisContext);

        List<Symbol> groupBy = analyzeGroupBy(
            selectAnalysis,
            node.getGroupBy(),
            expressionAnalyzer,
            expressionAnalysisContext);

        if (!node.getGroupBy().isEmpty() || expressionAnalysisContext.hasAggregates()) {
            ensureNonAggregatesInGroupBy(symbolPrinter, selectAnalysis.outputSymbols(), groupBy);
        }

        boolean isDistinct = node.getSelect().isDistinct();
        Symbol querySymbol = expressionAnalyzer.generateQuerySymbol(node.getWhere(), expressionAnalysisContext);
        WhereClause whereClause = new WhereClause(querySymbol);
        QuerySpec querySpec = new QuerySpec()
            .orderBy(analyzeOrderBy(
                selectAnalysis,
                node.getOrderBy(),
                expressionAnalyzer,
                expressionAnalysisContext,
                expressionAnalysisContext.hasAggregates() || ((groupBy != null && !groupBy.isEmpty())),
                isDistinct))
            .having(analyzeHaving(
                node.getHaving(),
                groupBy,
                expressionAnalyzer,
                context.expressionAnalysisContext()))
            .limit(longSymbolOrNull(node.getLimit(), expressionAnalyzer, expressionAnalysisContext))
            .offset(longSymbolOrNull(node.getOffset(), expressionAnalyzer, expressionAnalysisContext))
            .outputs(selectAnalysis.outputSymbols())
            .where(whereClause)
            .groupBy(groupBy)
            .hasAggregates(expressionAnalysisContext.hasAggregates());

        QueriedRelation relation;
        if (context.sources().size() == 1) {
            AnalyzedRelation source = Iterables.getOnlyElement(context.sources().values());
            if (source instanceof QueriedRelation) {
                relation = new QueriedSelectRelation(
                    isDistinct,
                    (QueriedRelation) source,
                    selectAnalysis.outputNames(),
                    querySpec
                );
            } else {
                relation = new QueriedTable<>(
                    isDistinct,
                    (AbstractTableRelation<?>) source,
                    selectAnalysis.outputNames(),
                    querySpec
                );
            }
        } else {
            relation = new MultiSourceSelect(
                isDistinct,
                context.sources(),
                selectAnalysis.outputNames(),
                querySpec,
                context.joinPairs()
            );
        }
        statementContext.endRelation();
        return relation;
    }

    @Nullable
    private static Symbol longSymbolOrNull(Optional<Expression> optExpression,
                                           ExpressionAnalyzer expressionAnalyzer,
                                           ExpressionAnalysisContext expressionAnalysisContext) {
        if (optExpression.isPresent()) {
            Symbol symbol = expressionAnalyzer.convert(optExpression.get(), expressionAnalysisContext);
            return ExpressionAnalyzer.cast(symbol, DataTypes.LONG);
        }
        return null;
    }

    private static void ensureNonAggregatesInGroupBy(SymbolPrinter symbolPrinter,
                                                     List<Symbol> outputSymbols,
                                                     List<Symbol> groupBy) throws IllegalArgumentException {
        for (int i = 0; i < outputSymbols.size(); i++) {
            Symbol output = outputSymbols.get(i);
            if (groupBy == null || !groupBy.contains(output)) {
                if (output.symbolType().isValueSymbol()) {
                    // values are allowed even if not present in group by
                    continue;
                }

                if (Aggregations.containsAggregationOrscalar(output) == false ||
                    Aggregations.matchGroupBySymbol(output, groupBy) == false) {
                    String offendingSymbolName = symbolPrinter.printUnqualified(output);
                    if (output instanceof Function) {
                        Function function = (Function) output;
                        if (function.info().type() == FunctionInfo.Type.TABLE) {
                            // table function can occur in the outputs without being present in the GROUP BY
                            // if the arguments to it are within GROUP BY
                            ensureNonAggregatesInGroupBy(symbolPrinter, function.arguments(), groupBy);
                            return;
                        }
                    }
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH,
                            "'%s' must appear in the GROUP BY clause " +
                            "or be used in an aggregation function." +
                            " Perhaps you grouped by an alias that clashes with a column in the relations",
                            offendingSymbolName
                        ));
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
                default:
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
            Symbol symbol = symbolFromExpressionFallbackOnSelectOutput(
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
            if (!expressionAnalysisContext.hasAggregates() && (groupBy == null || groupBy.isEmpty())) {
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
     * in order by clauses it is possible to reference anything in the select list by using a number or alias
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
        if (expression instanceof QualifiedNameReference) {
            Symbol symbol = tryGetFromSelectList((QualifiedNameReference) expression, selectAnalysis);
            if (symbol != null) {
                return symbol;
            }
        }
        Symbol symbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);
        if (symbol.symbolType().isValueSymbol()) {
            symbol = getByPosition(selectAnalysis.outputSymbols(), symbol, clause);
        }
        return symbol;
    }

    @Nullable
    private static Symbol tryGetFromSelectList(QualifiedNameReference expression, SelectAnalysis selectAnalysis) {
        List<String> parts = expression.getName().getParts();
        if (parts.size() == 1) {
            Symbol symbol = getOneOrAmbiguous(selectAnalysis.outputMultiMap(), Iterables.getOnlyElement(parts));
            if (symbol != null) {
                return symbol;
            }
        }
        return null;
    }


    /**
     * Resolve expression by also taking ordinal reference into account (eg. for `GROUP BY` clauses).
     * In case we cannot resolve the expression because an alias is used, will try to resolve the alias.
     * <p>
     * NOTE: in case an alias with the same name as a real column is used, we will take the column value into account
     */
    private static Symbol symbolFromExpressionFallbackOnSelectOutput(Expression expression,
                                                                     SelectAnalysis selectAnalysis,
                                                                     String clause,
                                                                     ExpressionAnalyzer expressionAnalyzer,
                                                                     ExpressionAnalysisContext expressionAnalysisContext) {
        try {
            Symbol symbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);
            if (symbol.symbolType().isValueSymbol()) {
                return getByPosition(selectAnalysis.outputSymbols(), symbol, clause);
            }
            return symbol;
        } catch (ColumnUnknownException e) {
            if (expression instanceof QualifiedNameReference) {
                Symbol symbol = tryGetFromSelectList((QualifiedNameReference) expression, selectAnalysis);
                if (symbol != null) {
                    return symbol;
                }
            }
            throw e;
        }
    }

    private static Symbol getByPosition(List<Symbol> outputSymbols, Symbol ordinal, String clause) {
        Literal literal;
        try {
            literal = io.crate.expression.symbol.Literal.convert(ordinal, DataTypes.INTEGER);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot use %s in %s clause", SymbolPrinter.INSTANCE.printUnqualified(ordinal), clause));
        }
        Object ord = literal.value();
        if (ord == null) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot use %s in %s clause", SymbolPrinter.INSTANCE.printUnqualified(ordinal), clause));
        }
        return ordinalOutputReference(outputSymbols, (int) ord, clause);
    }

    private static Symbol ordinalOutputReference(List<Symbol> outputSymbols, int ordinal, String clauseName) {
        // SQL has 1 based array access instead of 0 based.
        int idx = ordinal - 1;
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
            throw new AmbiguousColumnAliasException(key, symbols);
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
        QualifiedName tableQualifiedName = node.getName();
        SearchPath searchPath = context.sessionContext().searchPath();
        RelationName relationName;
        AnalyzedRelation relation;
        TableInfo tableInfo;
        try {
            tableInfo = schemas.resolveTableInfo(tableQualifiedName, context.currentOperation(), searchPath);
            if (tableInfo instanceof DocTableInfo) {
                // Dispatching of doc relations is based on the returned class of the schema information.
                relation = new DocTableRelation((DocTableInfo) tableInfo);
                relationName = tableInfo.ident();
            } else {
                relation = new TableRelation(tableInfo);
                relationName = tableInfo.ident();
            }
        } catch (RelationUnknown e) {
            Tuple<ViewMetaData, RelationName> viewMetaData;
            try {
                viewMetaData = schemas.resolveView(tableQualifiedName, searchPath);
            } catch (RelationUnknown e1) {
                // don't shadow original exception, as looking for the view is just a fallback
                throw e;
            }
            ViewMetaData view = viewMetaData.v1();
            relationName = viewMetaData.v2();
            AnalyzedRelation resolvedView = process(SqlParser.createStatement(view.stmt()), context);
            if (!(resolvedView instanceof QueriedRelation)) {
                throw new IllegalArgumentException("View must be a top-level SELECT statement, got: " + view.stmt());
            }
            relation = new AnalyzedView(relationName, view.owner(), (QueriedRelation) resolvedView);
        }

        context.currentRelationContext().addSourceRelation(relationName.schema(), relationName.name(), relation);
        return relation;
    }

    @Override
    public AnalyzedRelation visitTableFunction(TableFunction node, StatementAnalysisContext statementContext) {
        RelationAnalysisContext context = statementContext.currentRelationContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            statementContext.transactionContext(),
            statementContext.convertParamFunction(),
            new FieldProvider() {
                @Override
                public Symbol resolveField(QualifiedName qualifiedName, @Nullable List path, Operation operation) {
                    throw new UnsupportedOperationException("Can only resolve literals");
                }
            },
            null
        );

        Symbol symbol = expressionAnalyzer.convert(node.functionCall(), context.expressionAnalysisContext());

        if (!(symbol instanceof Function)) {
            throw new UnsupportedOperationException(
                String.format(
                    Locale.ENGLISH,
                    "Non table function '%s' is not supported in from clause", node.name()));
        }
        Function function = (Function) symbol;
        FunctionIdent ident = function.info().ident();
        FunctionImplementation functionImplementation = functions.getQualified(ident);
        if (functionImplementation.info().type() != FunctionInfo.Type.TABLE) {
            throw new UnsupportedOperationException(
                String.format(
                    Locale.ENGLISH,
                    "Non table function '%s' is not supported in from clause", ident.name()));
        }
        TableFunctionImplementation tableFunction = (TableFunctionImplementation) functionImplementation;
        TableInfo tableInfo = tableFunction.createTableInfo();
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
