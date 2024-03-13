/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import static io.crate.common.collections.Iterables.getOnlyElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.JoinRelation;
import io.crate.analyze.OrderBy;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.relations.select.SelectAnalyzer;
import io.crate.analyze.validator.GroupBySymbolValidator;
import io.crate.analyze.validator.HavingSymbolValidator;
import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.common.collections.Lists;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.GroupAndAggregateSemantics;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.tablefunctions.TableFunctionFactory;
import io.crate.expression.tablefunctions.ValuesFunction;
import io.crate.fdw.ForeignTable;
import io.crate.fdw.ForeignTableRelation;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.metadata.view.ViewInfo;
import io.crate.planner.consumer.OrderByWithAggregationValidator;
import io.crate.planner.consumer.RelationNameCollector;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Except;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.Intersect;
import io.crate.sql.tree.Join;
import io.crate.sql.tree.JoinCriteria;
import io.crate.sql.tree.JoinOn;
import io.crate.sql.tree.JoinUsing;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.Relation;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableFunction;
import io.crate.sql.tree.TableSubquery;
import io.crate.sql.tree.Union;
import io.crate.sql.tree.Values;
import io.crate.sql.tree.ValuesList;
import io.crate.sql.tree.WithQuery;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

@Singleton
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RelationAnalyzer extends DefaultTraversalVisitor<AnalyzedRelation, StatementAnalysisContext> {

    private final NodeContext nodeCtx;
    private final Schemas schemas;

    private static final List<Relation> EMPTY_ROW_TABLE_RELATION = List.of(
        new TableFunction(new FunctionCall(QualifiedName.of("empty_row"), Collections.emptyList()))
    );

    @Inject
    public RelationAnalyzer(NodeContext nodeCtx, Schemas schemas) {
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
    }

    public AnalyzedRelation analyze(Node node, StatementAnalysisContext statementContext) {
        return node.accept(this, statementContext);
    }

    public AnalyzedRelation analyze(Query query,
                                    CoordinatorTxnCtx coordinatorTxnCtx,
                                    ParamTypeHints paramTypeHints) {
        return analyze(query, new StatementAnalysisContext(paramTypeHints, Operation.READ, coordinatorTxnCtx));
    }

    @Override
    protected AnalyzedRelation visitQuery(Query node, StatementAnalysisContext statementContext) {
        // Start scope shared by possible with-queries and the child relation
        statementContext.startRelation();
        if (node.getWith().isPresent()) {
            var with = node.getWith().get();
            with.withQueries().forEach(wq -> wq.accept(this, statementContext));
        }
        AnalyzedRelation childRelation = node.getQueryBody().accept(this, statementContext);
        statementContext.endRelation();

        if (node.getOrderBy().isEmpty() && node.getLimit().isEmpty() && node.getOffset().isEmpty()) {
            return childRelation;
        }
        // In case of Set Operation (UNION, INTERSECT EXCEPT) or VALUES clause,
        // the `node` contains the ORDER BY and/or LIMIT and/or OFFSET and wraps the
        // actual operation (eg: UNION) which is parsed into the `queryBody` of the `node`.

        // Use child relation to process expressions of the "root" Query node
        statementContext.startRelation();
        RelationAnalysisContext relationAnalysisContext = statementContext.currentRelationContext();
        relationAnalysisContext.addSourceRelation(childRelation);
        statementContext.endRelation();

        List<Symbol> childRelationFields = childRelation.outputs();
        var coordinatorTxnCtx = statementContext.transactionContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            nodeCtx,
            statementContext.paramTyeHints(),
            new FullQualifiedNameFieldProvider(
                relationAnalysisContext.sources(),
                relationAnalysisContext.parentSources(),
                coordinatorTxnCtx.sessionSettings().searchPath().currentSchema()),
            new SubqueryAnalyzer(this, statementContext));
        ExpressionAnalysisContext expressionAnalysisContext = relationAnalysisContext.expressionAnalysisContext();
        SelectAnalysis selectAnalysis = new SelectAnalysis(
            childRelationFields.size(),
            relationAnalysisContext.sources(),
            expressionAnalyzer,
            expressionAnalysisContext);
        for (Symbol field : childRelationFields) {
            selectAnalysis.add(Symbols.pathFromSymbol(field), field);
        }

        var normalizer = EvaluatingNormalizer.functionOnlyNormalizer(
            nodeCtx,
            f -> expressionAnalysisContext.isEagerNormalizationAllowed() && f.signature().isDeterministic()
        );

        return new QueriedSelectRelation(
            false,
            List.of(childRelation),
            List.of(),
            selectAnalysis.outputSymbols(),
            Literal.BOOLEAN_TRUE,
            List.of(),
            null,
            analyzeOrderBy(
                selectAnalysis,
                node.getOrderBy(),
                expressionAnalyzer,
                expressionAnalysisContext,
                false,
                false
            ),
            longSymbolOrNull(
                node.getLimit(),
                expressionAnalyzer,
                expressionAnalysisContext,
                normalizer,
                coordinatorTxnCtx
            ),
            longSymbolOrNull(
                node.getOffset(),
                expressionAnalyzer,
                expressionAnalysisContext,
                normalizer,
                coordinatorTxnCtx
            )
        );
    }

    @Override
    protected AnalyzedRelation visitUnion(Union node, StatementAnalysisContext context) {
        AnalyzedRelation left = node.getLeft().accept(this, context);
        AnalyzedRelation right = node.getRight().accept(this, context);

        return new UnionSelect(left, right, node.isDistinct());
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
        AnalyzedRelation leftRel = node.getLeft().accept(this, statementContext);
        AnalyzedRelation rightRel = node.getRight().accept(this, statementContext);

        Optional<JoinCriteria> optCriteria = node.getCriteria();
        Symbol joinCondition = null;
        if (optCriteria.isPresent()) {
            JoinCriteria joinCriteria = optCriteria.get();
            Expression expression;
            if (joinCriteria instanceof JoinOn joinOn) {
                expression = joinOn.getExpression();
            } else if (joinCriteria instanceof JoinUsing joinUsing) {
                expression = validateAndExtractFromUsing(leftRel.outputs(), rightRel.outputs(), joinUsing);
            } else {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "join criteria %s not supported",
                        joinCriteria.getClass().getSimpleName()));
            }
            try {
                RelationAnalysisContext relationContext = statementContext.currentRelationContext();
                final CoordinatorTxnCtx coordinatorTxnCtx = statementContext.transactionContext();
                ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                    coordinatorTxnCtx,
                    nodeCtx,
                    statementContext.paramTyeHints(),
                    new FullQualifiedNameFieldProvider(
                        relationContext.sources(),
                        relationContext.parentSources(),
                        coordinatorTxnCtx.sessionSettings().searchPath().currentSchema()),
                    new SubqueryAnalyzer(this, statementContext));
                var expressionAnalysisContext = statementContext.currentRelationContext().expressionAnalysisContext();
                joinCondition = expressionAnalyzer.convert(expression, expressionAnalysisContext);
            } catch (RelationUnknown e) {
                throw new RelationValidationException(
                    e.getTableIdents(),
                    String.format(Locale.ENGLISH, "missing FROM-clause entry for relation '%s'", e.getTableIdents())
                );
            }
        }
        JoinRelation joinRelation = new JoinRelation(leftRel, rightRel, node.getType(), joinCondition);
        JoinPair joinPair = extractJoinPair(joinRelation, statementContext.currentRelationContext().sourceNames());
        statementContext.currentRelationContext().addJoinPair(joinPair);
        return joinRelation;
    }

    private static JoinPair extractJoinPair(JoinRelation joinRelation, List<RelationName> relevantRelationsInOrder) {
        assert relevantRelationsInOrder.size() >= 2 : "sources must be added first, cannot add join pair for only 1 source";

        var joinCondition = joinRelation.joinCondition();
        if (joinCondition != null) {
            Set<RelationName> relationsInJoinConditions = RelationNameCollector.collect(joinCondition);
            if (relationsInJoinConditions.size() > 1) {
                // We have join conditions with two relations or more. The join conditions such as
                // `t1.x = t2.x` determines which relations are joined together.
                // For this reason we keep only the relations which are part of the join conditions.
                // This makes sure we don't pick the wrong relations at the right/left assignment
                // for the join pair but keep the original order.
                relevantRelationsInOrder.retainAll(relationsInJoinConditions);
            }
        }
        var left = relevantRelationsInOrder.get(relevantRelationsInOrder.size() - 2);
        var right = relevantRelationsInOrder.get(relevantRelationsInOrder.size() - 1);
        return JoinPair.of(left, right, joinRelation.joinType(), joinCondition);
    }

    private static Expression validateAndExtractFromUsing(List<Symbol> leftOutputs,
                                                          List<Symbol> rightOutputs,
                                                          JoinUsing joinUsing) {
        var lhsOutputs = new HashMap<String, Symbol>();
        var rhsOutputs = new HashMap<String, Symbol>();

        for (var joinColumn : joinUsing.getColumns()) {

            boolean joinColumnExistsInLeft = false;
            for (var leftOutput : leftOutputs) {
                var columnIdent = Symbols.pathFromSymbol(leftOutput);
                if (columnIdent.name().equals(joinColumn)) {
                    joinColumnExistsInLeft = true;
                    if (lhsOutputs.put(joinColumn, leftOutput) != null) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                         "common column name %s appears more than once in left table", joinColumn));
                    }
                }
            }

            if (!joinColumnExistsInLeft) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                 "column %s specified in USING clause does not exist in left table", joinColumn));
            }

            boolean joinColumnExistsInRight = false;
            for (Symbol rightOutput : rightOutputs) {
                var columnIdent = Symbols.pathFromSymbol(rightOutput);
                if (columnIdent.name().equals(joinColumn)) {
                    joinColumnExistsInRight = true;
                    if (rhsOutputs.put(joinColumn, rightOutput) != null) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                         "common column name %s appears more than once in right table", joinColumn));
                    } else {
                        var lhsType = lhsOutputs.get(joinColumn).valueType();
                        var rhsType = rightOutput.valueType();
                        if (lhsType.equals(rhsType) == false) {
                            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                             "JOIN/USING types %s and %s varying cannot be matched", lhsType.getName(), rhsType.getName())
                            );
                        }
                    }
                }
            }

            if (!joinColumnExistsInRight) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                 "column %s specified in USING clause does not exist in right table", joinColumn));
            }
        }

        var lhsRelationNames = new HashSet<RelationName>();
        var rhsRelationNames = new HashSet<RelationName>();

        for (Symbol symbol : lhsOutputs.values()) {
            lhsRelationNames.addAll(RelationNameCollector.collect(symbol));
        }

        for (Symbol symbol : rhsOutputs.values()) {
            rhsRelationNames.addAll(RelationNameCollector.collect(symbol));
        }

        return JoinUsing.toExpression(
            getOnlyElement(lhsRelationNames).toQualifiedName(),
            getOnlyElement(rhsRelationNames).toQualifiedName(),
            joinUsing.getColumns()
        );
    }

    @Override
    protected AnalyzedRelation visitQuerySpecification(QuerySpecification node, StatementAnalysisContext statementContext) {
        List<Relation> from = node.getFrom().isEmpty() ? EMPTY_ROW_TABLE_RELATION : node.getFrom();
        RelationAnalysisContext currentRelationContext = statementContext.startRelation();

        for (Relation relation : from) {
            // different from relations have to be isolated from each other
            RelationAnalysisContext innerContext = statementContext.startRelation();
            relation.accept(this, statementContext);
            statementContext.endRelation();
            for (Map.Entry<RelationName, AnalyzedRelation> entry : innerContext.sources().entrySet()) {
                currentRelationContext.addSourceRelation(entry.getValue());
            }
            for (JoinPair joinPair : innerContext.joinPairs()) {
                currentRelationContext.addJoinPair(joinPair);
            }
        }

        RelationAnalysisContext context = statementContext.currentRelationContext();
        CoordinatorTxnCtx coordinatorTxnCtx = statementContext.transactionContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            nodeCtx,
            statementContext.paramTyeHints(),
            new FullQualifiedNameFieldProvider(
                context.sources(),
                context.parentSources(),
                coordinatorTxnCtx.sessionSettings().searchPath().currentSchema()),
            new SubqueryAnalyzer(this, statementContext));

        ExpressionAnalysisContext expressionAnalysisContext = context.expressionAnalysisContext();
        expressionAnalysisContext.windows(node.getWindows());

        SelectAnalysis selectAnalysis = SelectAnalyzer.analyzeSelectItems(
            node.getSelect().getSelectItems(),
            context.sources(),
            expressionAnalyzer,
            expressionAnalysisContext);

        List<Symbol> groupBy = analyzeGroupBy(
            selectAnalysis,
            node.getGroupBy(),
            expressionAnalyzer,
            expressionAnalysisContext);

        if (!node.getGroupBy().isEmpty() || expressionAnalysisContext.hasAggregates()) {
            GroupAndAggregateSemantics.validate(selectAnalysis.outputSymbols(), groupBy);
        }

        boolean isDistinct = node.getSelect().isDistinct();
        Symbol where = expressionAnalyzer.generateQuerySymbol(node.getWhere(), expressionAnalysisContext);
        WhereClauseValidator.validate(where);

        var normalizer = EvaluatingNormalizer.functionOnlyNormalizer(
            nodeCtx,
            f -> expressionAnalysisContext.isEagerNormalizationAllowed() && f.signature().isDeterministic()
        );

        QueriedSelectRelation relation = new QueriedSelectRelation(
            isDistinct,
            List.copyOf(context.sources().values()),
            context.joinPairs(),
            selectAnalysis.outputSymbols(),
            where,
            groupBy,
            analyzeHaving(
                node.getHaving(),
                groupBy,
                expressionAnalyzer,
                context.expressionAnalysisContext()
            ),
            analyzeOrderBy(
                selectAnalysis,
                node.getOrderBy(),
                expressionAnalyzer,
                expressionAnalysisContext,
                expressionAnalysisContext.hasAggregates() || !groupBy.isEmpty(),
                isDistinct
            ),
            longSymbolOrNull(
                node.getLimit(),
                expressionAnalyzer,
                expressionAnalysisContext,
                normalizer,
                coordinatorTxnCtx),
            longSymbolOrNull(
                node.getOffset(),
                expressionAnalyzer,
                expressionAnalysisContext,
                normalizer,
                coordinatorTxnCtx
            )
        );
        statementContext.endRelation();
        return relation;
    }

    @Nullable
    private static Symbol longSymbolOrNull(Optional<Expression> optExpression,
                                           ExpressionAnalyzer expressionAnalyzer,
                                           ExpressionAnalysisContext expressionAnalysisContext,
                                           EvaluatingNormalizer normalizer,
                                           CoordinatorTxnCtx coordinatorTxnCtx) {
        if (optExpression.isPresent()) {
            Symbol symbol = expressionAnalyzer.convert(optExpression.get(), expressionAnalysisContext);
            return normalizer.normalize(symbol.cast(DataTypes.LONG), coordinatorTxnCtx);
        }
        return null;
    }

    @Nullable
    private static OrderBy analyzeOrderBy(SelectAnalysis selectAnalysis,
                                          List<SortItem> orderBy,
                                          ExpressionAnalyzer expressionAnalyzer,
                                          ExpressionAnalysisContext expressionAnalysisContext,
                                          boolean hasAggregatesOrGrouping,
                                          boolean isDistinct) {

        var selectListExpressionAnalyzer = expressionAnalyzer.referenceSelect(selectAnalysis);
        return OrderyByAnalyzer.analyzeSortItems(orderBy, sortKey -> {
            Symbol symbol = symbolFromSelectOutputReferenceOrExpression(
                sortKey, selectAnalysis, "ORDER BY", selectListExpressionAnalyzer, expressionAnalysisContext);

            SemanticSortValidator.validate(symbol);
            if (hasAggregatesOrGrouping) {
                OrderByWithAggregationValidator.validate(symbol, selectAnalysis.outputSymbols(), isDistinct);
            }
            return symbol;
        });
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

    @Nullable
    private Symbol analyzeHaving(Optional<Expression> having,
                                 @Nullable List<Symbol> groupBy,
                                 ExpressionAnalyzer expressionAnalyzer,
                                 ExpressionAnalysisContext expressionAnalysisContext) {
        if (having.isPresent()) {
            if (!expressionAnalysisContext.hasAggregates() && (groupBy == null || groupBy.isEmpty())) {
                throw new IllegalArgumentException("HAVING clause can only be used in GROUP BY or global aggregate queries");
            }
            Symbol symbol = expressionAnalyzer.convert(having.get(), expressionAnalysisContext);
            HavingSymbolValidator.validate(symbol, groupBy);
            return symbol;
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
     *     select [1, 2] as arr ... order by arr[1]
     * </pre>
     */
    private static Symbol symbolFromSelectOutputReferenceOrExpression(Expression expression,
                                                                      SelectAnalysis selectAnalysis,
                                                                      String clause,
                                                                      ExpressionAnalyzer expressionAnalyzer,
                                                                      ExpressionAnalysisContext expressionAnalysisContext) {
        int ord = -1;
        if (expression instanceof IntegerLiteral intLiteral) {
            ord = intLiteral.getValue();
        } else if (expression instanceof LongLiteral longLiteral) {
            try {
                ord = DataTypes.INTEGER.sanitizeValue(longLiteral.getValue());
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Cannot use %s in %s clause", longLiteral, clause));
            }
        }
        if (ord > -1) {
            return ordinalOutputReference(selectAnalysis.outputSymbols(), ord, clause);
        }
        Symbol symbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);
        return symbol;
    }

    @Nullable
    private static Symbol tryGetFromSelectList(QualifiedNameReference expression, SelectAnalysis selectAnalysis) {
        List<String> parts = expression.getName().getParts();
        if (parts.size() == 1) {
            return SelectListFieldProvider.getOneOrAmbiguous(selectAnalysis.outputMultiMap(), getOnlyElement(parts));
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
            if (symbol instanceof Literal) {
                return getByPosition(selectAnalysis.outputSymbols(), (Literal<?>) symbol, clause);
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

    private static Symbol getByPosition(List<Symbol> outputSymbols, Literal<?> ordinal, String clause) {
        if (ordinal.valueType().equals(DataTypes.UNDEFINED)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot use %s in %s clause", ordinal, clause));
        }
        Integer ord;
        try {
            ord = DataTypes.INTEGER.sanitizeValue(ordinal.value());
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot use %s in %s clause", ordinal, clause));
        }
        if (ord == null) {
            // It's NULL ordinal explicitly casted to some type, we allow it in GROUP BY or ORDER BY clauses to align with PG.
            return ordinal;
        }
        return ordinalOutputReference(outputSymbols, ord, clause);
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


    @Override
    protected AnalyzedRelation visitAliasedRelation(AliasedRelation node, StatementAnalysisContext context) {
        context.startRelation(true);
        AnalyzedRelation childRelation = node.getRelation().accept(this, context);
        List<String> columnAliases = node.getColumnNames();
        if (node.getColumnNames().isEmpty()) {
            if (childRelation instanceof TableFunctionRelation tableFunctionRelation) {
                // Values function is not a table function so it should not use the table alias
                // see https://github.com/crate/crate/pull/11348
                if (!Objects.equals(tableFunctionRelation.function().signature(), ValuesFunction.SIGNATURE)) {
                    if (tableFunctionRelation.outputs().size() == 1) {
                        columnAliases = List.of(node.getAlias());
                    }
                }
            }
        }
        AnalyzedRelation aliasedRelation = new AliasedAnalyzedRelation(
            childRelation,
            new RelationName(null, node.getAlias()),
            columnAliases
        );
        context.endRelation();
        context.currentRelationContext().addSourceRelation(aliasedRelation);
        return aliasedRelation;
    }

    @Override
    public AnalyzedRelation visitWithQuery(WithQuery node, StatementAnalysisContext context) {
        // With queries are already analyzed inside a dedicated relation context, no need for a new one.
        AnalyzedRelation childRelation = node.query().accept(this, context);
        AnalyzedRelation aliasedRelation = new AliasedAnalyzedRelation(
            childRelation,
            new RelationName(null, node.name()),
            node.columnNames()
        );
        context.currentRelationContext().addSourceRelation(aliasedRelation);
        return aliasedRelation;
    }

    @Override
    protected AnalyzedRelation visitTable(Table<?> node, StatementAnalysisContext context) {
        QualifiedName tableQualifiedName = node.getName();
        SearchPath searchPath = context.sessionSettings().searchPath();
        AnalyzedRelation relation;
        var relationContext = context.currentRelationContext();

        var withQuery = relationContext.parentSources()
            .getAncestor(RelationName.of(tableQualifiedName, null));
        if (withQuery != null) {
            relation = withQuery;
        } else {
            RelationInfo relationInfo = schemas.findRelation(
                tableQualifiedName, context.currentOperation(), context.sessionSettings().sessionUser(), searchPath);
            switch (relationInfo) {
                case DocTableInfo docTable ->
                    // Dispatching of doc relations is based on the returned class of the schema information.
                    relation = new DocTableRelation(docTable);
                case ForeignTable table -> relation = new ForeignTableRelation(table);
                case TableInfo table -> relation = new TableRelation(table);
                case ViewInfo viewInfo -> {
                    Statement viewQuery = SqlParser.createStatement(viewInfo.definition());
                    AnalyzedRelation resolvedView = context.withSearchPath(
                            viewInfo.searchPath(),
                            newContext -> viewQuery.accept(this, newContext)
                    );
                    relation = new AnalyzedView(viewInfo.ident(), viewInfo.owner(), resolvedView);
                }
                default -> throw new IllegalStateException("Unexpected relationInfo: " + relationInfo);
            }
        }
        relationContext.addSourceRelation(relation);
        return relation;
    }

    @Override
    public AnalyzedRelation visitTableFunction(TableFunction node, StatementAnalysisContext statementContext) {
        RelationAnalysisContext context = statementContext.currentRelationContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            statementContext.transactionContext(),
            nodeCtx,
            statementContext.paramTyeHints(),
            FieldProvider.UNSUPPORTED,
            null
        );

        ExpressionAnalysisContext expressionContext = context.expressionAnalysisContext();
        // we support `FROM scalar()` but not `FROM 'literal'` -> we turn off eager normalization
        // so we can distinguish between Function and Literal.
        final boolean allowEagerNormalizeOriginalValue = expressionContext.isEagerNormalizationAllowed();
        expressionContext.allowEagerNormalize(false);
        Symbol symbol = expressionAnalyzer.convert(node.functionCall(), expressionContext);
        expressionContext.allowEagerNormalize(allowEagerNormalizeOriginalValue);

        if (!(symbol instanceof Function)) {
            throw new UnsupportedOperationException(
                String.format(
                    Locale.ENGLISH,
                    "Symbol '%s' is not supported in FROM clause", node.name()));
        }
        Function function = (Function) symbol;
        FunctionImplementation functionImplementation = nodeCtx.functions().getQualified(function);
        assert functionImplementation != null : "Function implementation not found using full qualified lookup";
        TableFunctionImplementation<?> tableFunction = TableFunctionFactory.from(functionImplementation);
        TableFunctionRelation tableRelation = new TableFunctionRelation(tableFunction, function);
        context.addSourceRelation(tableRelation);
        return tableRelation;
    }

    @Override
    protected AnalyzedRelation visitTableSubquery(TableSubquery node, StatementAnalysisContext context) {
        if (!context.currentRelationContext().isAliasedRelation()) {
            throw new UnsupportedOperationException("subquery in FROM clause must have an alias");
        }
        return super.visitTableSubquery(node, context);
    }

    @Override
    public AnalyzedRelation visitValues(Values values, StatementAnalysisContext context) {
        var expressionAnalyzer = new ExpressionAnalyzer(
            context.transactionContext(),
            nodeCtx,
            context.paramTyeHints(),
            FieldProvider.UNSUPPORTED,
            new SubqueryAnalyzer(this, context)
        );
        var expressionAnalysisContext = new ExpressionAnalysisContext(context.sessionSettings());
        // prevent normalization of the values array, otherwise the value literals are converted to an array literal
        // and a special per-value-literal casting logic won't be executed (e.g. FloatLiteral.cast())
        expressionAnalysisContext.allowEagerNormalize(false);
        java.util.function.Function<Expression, Symbol> expressionToSymbol =
            e -> expressionAnalyzer.convert(e, expressionAnalysisContext);

        // There is a first pass to convert expressions from row oriented format:
        // `[[1, a], [2, b]]` to columns `[[1, 2], [a, b]]`
        //
        // At the same time we determine the column type with the highest precedence,
        // so that we don't fail with slight type miss-matches (long vs. int)
        List<ValuesList> rows = values.rows();
        assert rows.size() > 0 : "Parser grammar enforces at least 1 row";
        ValuesList firstRow = rows.get(0);
        int numColumns = firstRow.values().size();

        ArrayList<List<Symbol>> columns = new ArrayList<>();
        ArrayList<DataType<?>> targetTypes = new ArrayList<>(numColumns);
        var parentOutputColumns = context.parentOutputColumns();
        for (int c = 0; c < numColumns; c++) {
            ArrayList<Symbol> columnValues = new ArrayList<>();
            DataType<?> targetType;
            boolean usePrecedence = true;
            if (parentOutputColumns.size() > c) {
                targetType = parentOutputColumns.get(c).valueType();
                usePrecedence = false;
            } else {
                targetType = DataTypes.UNDEFINED;
            }
            for (int r = 0; r < rows.size(); r++) {
                List<Expression> row = rows.get(r).values();
                if (row.size() != numColumns) {
                    throw new IllegalArgumentException(
                        "VALUES lists must all be the same length. " +
                        "Found row with " + numColumns + " items and another with " + columns.size() + " items");
                }
                Symbol cell = expressionToSymbol.apply(row.get(c));
                columnValues.add(cell);

                var cellType = cell.valueType();
                if (r > 0 // skip first cell, we don't have to check for self-conversion
                    && !cellType.isConvertableTo(targetType, false)
                    && targetType.id() != DataTypes.UNDEFINED.id()) {
                    throw new IllegalArgumentException(
                        "The types of the columns within VALUES lists must match. " +
                        "Found `" + targetType + "` and `" + cellType + "` at position: " + c);
                }
                if (usePrecedence && cellType.precedes(targetType)) {
                    targetType = cellType;
                } else if (targetType == DataTypes.UNDEFINED) {
                    targetType = cellType;
                }
            }
            targetTypes.add(targetType);
            columns.add(columnValues);
        }

        var normalizer = EvaluatingNormalizer.functionOnlyNormalizer(
            nodeCtx,
            f -> f.signature().isDeterministic()
        );

        ArrayList<Symbol> arrays = new ArrayList<>(columns.size());
        for (int c = 0; c < numColumns; c++) {
            DataType<?> targetType = targetTypes.get(c);
            ArrayType<?> arrayType = new ArrayType<>(targetType);
            List<Symbol> columnValues = Lists.map(
                columns.get(c),
                s -> normalizer.normalize(s.cast(targetType), context.transactionContext())
            );
            arrays.add(new Function(
                ArrayFunction.SIGNATURE,
                columnValues,
                arrayType
            ));
        }
        FunctionImplementation implementation = nodeCtx.functions().getQualified(
            ValuesFunction.SIGNATURE,
            Symbols.typeView(arrays),
            RowType.EMPTY
        );
        Function function = new Function(
            implementation.signature(),
            arrays,
            RowType.EMPTY
        );

        TableFunctionImplementation<?> tableFunc = TableFunctionFactory.from(implementation);
        TableFunctionRelation relation = new TableFunctionRelation(tableFunc, function);
        context.startRelation();
        context.currentRelationContext().addSourceRelation(relation);
        context.endRelation();
        return relation;
    }
}
