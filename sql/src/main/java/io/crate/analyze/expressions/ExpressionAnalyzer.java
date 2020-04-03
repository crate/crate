/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.expressions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.action.sql.Option;
import io.crate.analyze.DataTypeAnalyzer;
import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.NegateLiterals;
import io.crate.analyze.OrderBy;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.SubscriptContext;
import io.crate.analyze.SubscriptValidator;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.OrderyByAnalyzer;
import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.operator.AllOperator;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.expression.operator.RegexpMatchOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.ExtractFunctions;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.SubscriptFunctions;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.scalar.arithmetic.NegateFunctions;
import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.expression.scalar.conditional.IfFunction;
import io.crate.expression.scalar.timestamp.CurrentTimestampFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.interval.IntervalParser;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.table.Operation;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArithmeticExpression;
import io.crate.sql.tree.ArrayComparison;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.ArraySubQueryExpression;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BetweenPredicate;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.IfExpression;
import io.crate.sql.tree.InListExpression;
import io.crate.sql.tree.InPredicate;
import io.crate.sql.tree.IntervalLiteral;
import io.crate.sql.tree.IsNotNullPredicate;
import io.crate.sql.tree.IsNullPredicate;
import io.crate.sql.tree.LikePredicate;
import io.crate.sql.tree.LogicalBinaryExpression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.MatchPredicate;
import io.crate.sql.tree.MatchPredicateColumnIdent;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NotExpression;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.RecordSubscript;
import io.crate.sql.tree.SearchedCaseExpression;
import io.crate.sql.tree.SimpleCaseExpression;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;
import io.crate.sql.tree.WhenClause;
import io.crate.sql.tree.Window;
import io.crate.sql.tree.WindowFrame;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UndefinedType;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.crate.sql.tree.IntervalLiteral.IntervalField.DAY;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.MINUTE;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.MONTH;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.SECOND;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.YEAR;

/**
 * <p>This Analyzer can be used to convert Expression from the SQL AST into symbols.</p>
 * <p>
 * <p>
 * In order to resolve QualifiedName or SubscriptExpressions it will use the fieldResolver given in the constructor and
 * generate a relationOutput for the matching Relation.
 * </p>
 */
public class ExpressionAnalyzer {

    private static final Map<ComparisonExpression.Type, ComparisonExpression.Type> SWAP_OPERATOR_TABLE =
        ImmutableMap.<ComparisonExpression.Type, ComparisonExpression.Type>builder()
            .put(ComparisonExpression.Type.GREATER_THAN, ComparisonExpression.Type.LESS_THAN)
            .put(ComparisonExpression.Type.LESS_THAN, ComparisonExpression.Type.GREATER_THAN)
            .put(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ComparisonExpression.Type.LESS_THAN_OR_EQUAL)
            .put(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL)
            .build();

    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final ParamTypeHints paramTypeHints;
    private final FieldProvider<?> fieldProvider;

    @Nullable
    private final SubqueryAnalyzer subQueryAnalyzer;
    private final Functions functions;
    private final InnerExpressionAnalyzer innerAnalyzer;
    private final Operation operation;

    private static final Pattern SUBSCRIPT_SPLIT_PATTERN = Pattern.compile("^([^\\.\\[]+)(\\.*)([^\\[]*)(\\['.*'\\])");

    public ExpressionAnalyzer(Functions functions,
                              CoordinatorTxnCtx coordinatorTxnCtx,
                              ParamTypeHints paramTypeHints,
                              FieldProvider<?> fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer) {
        this(functions, coordinatorTxnCtx, paramTypeHints, fieldProvider, subQueryAnalyzer, Operation.READ);
    }

    public ExpressionAnalyzer(Functions functions,
                              CoordinatorTxnCtx coordinatorTxnCtx,
                              ParamTypeHints paramTypeHints,
                              FieldProvider<?> fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer,
                              Operation operation) {
        this.functions = functions;
        this.coordinatorTxnCtx = coordinatorTxnCtx;
        this.paramTypeHints = paramTypeHints;
        this.fieldProvider = fieldProvider;
        this.subQueryAnalyzer = subQueryAnalyzer;
        this.innerAnalyzer = new InnerExpressionAnalyzer();
        this.operation = operation;
    }

    /**
     * Converts an expression into a symbol.
     *
     * Expressions like QualifiedName that reference a column are resolved using the fieldResolver that were passed
     * to the constructor.
     *
     * Some information (like resolved function symbols) are written onto the given expressionAnalysisContext.
     *
     * Functions with constants will be normalized.
     */
    public Symbol convert(Expression expression, ExpressionAnalysisContext expressionAnalysisContext) {
        return expression.accept(innerAnalyzer, expressionAnalysisContext);
    }

    public Symbol generateQuerySymbol(Optional<Expression> whereExpression, ExpressionAnalysisContext context) {
        return whereExpression.map(expression -> convert(expression, context)).orElse(Literal.BOOLEAN_TRUE);
    }

    private Symbol convertFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
        List<Symbol> arguments = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            Symbol argSymbol = expression.accept(innerAnalyzer, context);
            arguments.add(argSymbol);
        }

        List<String> parts = node.getName().getParts();
        // We don't set a default schema here because no supplied schema
        // means that we first try to lookup builtin functions, followed
        // by a lookup in the default schema for UDFs.
        String schema = null;
        String name;
        if (parts.size() == 1) {
            name = parts.get(0);
        } else {
            schema = parts.get(0);
            name = parts.get(1);
        }

        Symbol filter = node.filter()
            .map(expression -> convert(expression, context))
            .orElse(null);

        WindowDefinition windowDefinition = getWindowDefinition(node.getWindow(), context);
        if (node.isDistinct()) {
            if (arguments.size() > 1) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "%s(DISTINCT x) does not accept more than one argument", node.getName()));
            }
            Symbol collectSetFunction = allocateFunction(
                CollectSetAggregation.NAME,
                arguments,
                filter,
                context,
                functions,
                coordinatorTxnCtx);

            // define the outer function which contains the inner function as argument.
            String nodeName = "collection_" + name;
            List<Symbol> outerArguments = List.of(collectSetFunction);
            try {
                return allocateBuiltinOrUdfFunction(
                    schema, nodeName, outerArguments, null, windowDefinition, context);
            } catch (UnsupportedOperationException ex) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "unknown function %s(DISTINCT %s)", name, arguments.get(0).valueType()), ex);
            }
        } else {
            return allocateBuiltinOrUdfFunction(schema, name, arguments, filter, windowDefinition, context);
        }
    }

    @Nullable
    private WindowDefinition getWindowDefinition(Optional<Window> maybeWindow,
                                                 ExpressionAnalysisContext context) {
        if (!maybeWindow.isPresent()) {
            return null;
        }
        var unresolvedWindow = maybeWindow.get();

        final Window window;
        if (unresolvedWindow.windowRef() != null) {
            var refWindow = resolveWindowRef(unresolvedWindow.windowRef(), context.windows());
            window = unresolvedWindow.merge(refWindow);
        } else {
            window = unresolvedWindow;
        }

        List<Symbol> partitionSymbols = new ArrayList<>(window.getPartitions().size());
        for (Expression partition : window.getPartitions()) {
            Symbol symbol = convert(partition, context);
            SemanticSortValidator.validate(symbol, "PARTITION BY");
            partitionSymbols.add(symbol);
        }

        OrderBy orderBy = OrderyByAnalyzer.analyzeSortItems(window.getOrderBy(), sortKey -> {
            Symbol symbol = convert(sortKey, context);
            SemanticSortValidator.validate(symbol);
            return symbol;
        });

        WindowFrameDefinition windowFrameDefinition = WindowDefinition.RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW;
        if (window.getWindowFrame().isPresent()) {
            WindowFrame windowFrame = window.getWindowFrame().get();
            validateFrame(window, windowFrame);
            FrameBound start = windowFrame.getStart();
            FrameBoundDefinition startBound = convertToAnalyzedFrameBound(context, start);

            FrameBoundDefinition endBound = windowFrame.getEnd()
                .map(end -> convertToAnalyzedFrameBound(context, end))
                .orElse(new FrameBoundDefinition(FrameBound.Type.CURRENT_ROW, Literal.NULL));
            windowFrameDefinition = new WindowFrameDefinition(windowFrame.mode(), startBound, endBound);
        }

        return new WindowDefinition(partitionSymbols, orderBy, windowFrameDefinition);
    }

    /**
     * Resolved the window definition from the list of named windows.
     * Reduces the named list of window definitions to a single one if
     * they are inter-referenced.
     *
     * @param name    A reference to a window definition.
     * @param windows A map of named window definitions.
     * @return A {@link Window} from the named window
     *         definitions or a new window if the targeted window has
     *         references to other window definitions.
     * @throws IllegalArgumentException If the window definition is not found.
     */
    private Window resolveWindowRef(@Nonnull String name, Map<String, Window> windows) {
        var window = windows.get(name);
        if (window == null) {
            throw new IllegalArgumentException("Window " + name + " does not exist");
        }

        String windowRef = window.windowRef();
        while (windowRef != null) {
            Window refWindow = windows.get(windowRef);
            window = window.merge(refWindow);
            windowRef = refWindow.windowRef();
        }
        return window;
    }

    private void validateFrame(Window window, WindowFrame windowFrame) {
        FrameBound.Type startType = windowFrame.getStart().getType();
        if (startType.equals(FrameBound.Type.FOLLOWING) ||
            startType.equals(FrameBound.Type.UNBOUNDED_FOLLOWING)) {
            throw new IllegalStateException("Frame start cannot be " + startType);
        }

        if (windowFrame.mode() == WindowFrame.Mode.RANGE) {
            if (startType.equals(FrameBound.Type.PRECEDING) && windowFrame.getStart().getValue() != null) {
                if (window.getOrderBy().size() != 1) {
                    throw new IllegalStateException("RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
                }
            }
        }

        windowFrame.getEnd().ifPresent(frameBound -> {
            FrameBound.Type endType = frameBound.getType();
            if (endType.equals(FrameBound.Type.PRECEDING) ||
                endType.equals(FrameBound.Type.UNBOUNDED_PRECEDING)) {
                throw new IllegalStateException("Frame end cannot be " + endType);
            }

            if (windowFrame.mode() == WindowFrame.Mode.RANGE) {
                if (endType.equals(FrameBound.Type.FOLLOWING) && windowFrame.getEnd().get().getValue() != null) {
                    if (window.getOrderBy().size() != 1) {
                        throw new IllegalStateException("RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
                    }
                }
            }
        });
    }

    private FrameBoundDefinition convertToAnalyzedFrameBound(ExpressionAnalysisContext context, FrameBound frameBound) {
        Expression offsetExpression = frameBound.getValue();
        Symbol offsetSymbol = offsetExpression == null ? Literal.NULL : convert(offsetExpression, context);
        return new FrameBoundDefinition(frameBound.getType(), offsetSymbol);
    }

    /**
     * Casts a list of symbols to a given list of target types.
     * @param symbolsToCast A list of {@link Symbol}s to cast to the types listed in targetTypes.
     * @param targetTypes A list of {@link DataType}s to use as the new type of symbolsToCast.
     * @return A new list with the casted symbols.
     */
    private static List<Symbol> cast(List<Symbol> symbolsToCast, List<DataType> targetTypes) {
        Preconditions.checkState(symbolsToCast.size() == targetTypes.size(),
            "Given symbol list has to match the target type list.");
        int size = symbolsToCast.size();
        List<Symbol> castList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            castList.add(symbolsToCast.get(i).cast(targetTypes.get(i), false));
        }
        return castList;
    }

    @Nullable
    static String getQuotedSubscriptLiteral(String nodeName) {
        Matcher matcher = SUBSCRIPT_SPLIT_PATTERN.matcher(nodeName);
        if (matcher.matches()) {
            StringBuilder quoted = new StringBuilder();
            String group1 = matcher.group(1);
            if (!group1.isEmpty()) {
                quoted.append("\"").append(group1).append("\"");
            } else {
                quoted.append(group1);
            }
            String group2 = matcher.group(2);
            String group3 = matcher.group(3);
            if (!group2.isEmpty() && !group3.isEmpty()) {
                quoted.append(matcher.group(2));
                quoted.append("\"").append(group3).append("\"");
            } else if (!group2.isEmpty()) {
                return null;
            }
            quoted.append(matcher.group(4));
            return quoted.toString();
        } else {
            return null;
        }
    }

    private class InnerExpressionAnalyzer extends AstVisitor<Symbol, ExpressionAnalysisContext> {

        @Override
        protected Symbol visitNode(Node node, ExpressionAnalysisContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Unsupported node %s", node));
        }

        @Override
        protected Symbol visitExpression(Expression node, ExpressionAnalysisContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Unsupported expression %s", ExpressionFormatter.formatStandaloneExpression(node)));
        }

        @Override
        protected Symbol visitCurrentTime(CurrentTime node, ExpressionAnalysisContext context) {
            if (!node.getType().equals(CurrentTime.Type.TIMESTAMP)) {
                visitExpression(node, context);
            }
            List<Symbol> args = List.of(
                Literal.of(node.getPrecision().orElse(CurrentTimestampFunction.DEFAULT_PRECISION))
            );
            return allocateFunction(CurrentTimestampFunction.NAME, args, context);
        }

        @Override
        protected Symbol visitIfExpression(IfExpression node, ExpressionAnalysisContext context) {
            // check for global operand
            Optional<Expression> defaultExpression = node.getFalseValue();
            List<Symbol> arguments = new ArrayList<>(defaultExpression.isPresent() ? 3 : 2);

            arguments.add(node.getCondition().accept(innerAnalyzer, context));
            arguments.add(node.getTrueValue().accept(innerAnalyzer, context));
            if (defaultExpression.isPresent()) {
                arguments.add(defaultExpression.get().accept(innerAnalyzer, context));
            }
            return allocateFunction(IfFunction.NAME, arguments, context);
        }

        @Override
        protected Symbol visitFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
            // If it's subscript function then use the special handling
            // and validation that is used for the subscript operator `[]`
            if (node.getName().toString().equalsIgnoreCase(SubscriptFunction.NAME)) {
                assert node.getArguments().size() == 2 : "Number of arguments for subscript function must be 2";
                return visitSubscriptExpression(
                    new SubscriptExpression(node.getArguments().get(0), node.getArguments().get(1)), context);
            }
            return convertFunctionCall(node, context);
        }

        @Override
        protected Symbol visitSimpleCaseExpression(SimpleCaseExpression node, ExpressionAnalysisContext context) {
            /* CASE caseOperand
             *   WHEN whenOperand1 THEN result1
             *   WHEN whenOperand2 THEN result2
             *   ELSE default
             * END
             * ---
             * operands: [eq(caseOperand, whenOperand1), eq(caseOperand, whenOperand2)]
             * results:  [result1, result2]
             */
            List<WhenClause> whenClauses = node.getWhenClauses();
            List<Symbol> operands = new ArrayList<>(whenClauses.size());
            List<Symbol> results = new ArrayList<>(whenClauses.size());
            Symbol caseOperand = convert(node.getOperand(), context);
            for (WhenClause whenClause : whenClauses) {
                Symbol whenOperand = convert(whenClause.getOperand(), context);
                operands.add(allocateFunction(EqOperator.NAME, ImmutableList.of(caseOperand, whenOperand), context));
                results.add(convert(whenClause.getResult(), context));
            }
            ensureResultTypesMatch(results);
            Expression defaultValue = node.getDefaultValue();
            return createChain(
                operands,
                results,
                defaultValue == null ? null : convert(defaultValue, context),
                context
            );
        }

        @Override
        protected Symbol visitSearchedCaseExpression(SearchedCaseExpression node, ExpressionAnalysisContext context) {
            List<WhenClause> whenClauses = node.getWhenClauses();
            List<Symbol> operands = new ArrayList<>(whenClauses.size());
            List<Symbol> results = new ArrayList<>(whenClauses.size());
            for (WhenClause whenClause : whenClauses) {
                operands.add(whenClause.getOperand().accept(innerAnalyzer, context));
                results.add(whenClause.getResult().accept(innerAnalyzer, context));
            }
            ensureResultTypesMatch(results);
            Expression defaultValue = node.getDefaultValue();
            return createChain(operands, results, defaultValue == null ? null : convert(defaultValue, context), context);
        }

        /**
         * Create a chain of if functions by the given list of operands and results.
         *
         * @param operands              List of condition symbols, all must result in a boolean value.
         * @param results               List of result symbols to return if corresponding condition evaluates to true.
         * @param defaultValueSymbol    Default symbol to return if all conditions evaluates to false.
         * @param context               The ExpressionAnalysisContext
         * @return                      Returns the first {@link IfFunction} of the chain.
         */
        private Symbol createChain(List<Symbol> operands,
                                   List<Symbol> results,
                                   @Nullable Symbol defaultValueSymbol,
                                   ExpressionAnalysisContext context) {
            Symbol lastSymbol = defaultValueSymbol;
            // process operands in reverse order
            for (int i = operands.size() - 1 ; i >= 0; i--) {
                Symbol operand = operands.get(i);
                Symbol result = results.get(i);
                List<Symbol> arguments = Lists.newArrayList(operand, result);
                if (lastSymbol != null) {
                    arguments.add(lastSymbol);
                }
                lastSymbol = allocateFunction(IfFunction.NAME, arguments, context);
            }
            return lastSymbol;
        }

        @Override
        protected Symbol visitCast(Cast node, ExpressionAnalysisContext context) {
            DataType<?> returnType = DataTypeAnalyzer.convert(node.getType());
            return node.getExpression().accept(this, context).cast(returnType, false);
        }

        @Override
        protected Symbol visitTryCast(TryCast node, ExpressionAnalysisContext context) {
            DataType<?> returnType = DataTypeAnalyzer.convert(node.getType());

            if (CastFunctionResolver.supportsExplicitConversion(returnType)) {
                try {
                    return node.getExpression().accept(this, context).cast(returnType, true);
                } catch (ConversionException e) {
                    return Literal.NULL;
                }
            }
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "No cast function found for return type %s", returnType.getName()));
        }

        @Override
        protected Symbol visitExtract(Extract node, ExpressionAnalysisContext context) {
            Symbol expression = node.getExpression().accept(this, context);
            return allocateFunction(
                ExtractFunctions.functionNameFrom(node.getField()),
                List.of(expression),
                context);
        }

        @Override
        protected Symbol visitInPredicate(InPredicate node, ExpressionAnalysisContext context) {
            /*
             * convert where x IN (values)
             *
             * where values = a list of expressions or a subquery
             *
             * into
             *
             *      x = ANY(array(1, 2, 3, ...))
             * or
             *      x = ANY(select x from t)
             */
            final Expression arrayExpression;
            Expression valueList = node.getValueList();
            if (valueList instanceof InListExpression) {
                List<Expression> expressions = ((InListExpression) valueList).getValues();
                arrayExpression = new ArrayLiteral(expressions);
            } else {
                arrayExpression = node.getValueList();
            }
            ArrayComparisonExpression arrayComparisonExpression =
                new ArrayComparisonExpression(ComparisonExpression.Type.EQUAL,
                    ArrayComparison.Quantifier.ANY,
                    node.getValue(),
                    arrayExpression);
            return arrayComparisonExpression.accept(this, context);
        }

        @Override
        protected Symbol visitArraySubQueryExpression(ArraySubQueryExpression node, ExpressionAnalysisContext context) {
            SubqueryExpression subqueryExpression = node.subqueryExpression();
            context.registerArrayChild(subqueryExpression);
            return subqueryExpression.accept(this, context);
        }

        @Override
        protected Symbol visitIsNotNullPredicate(IsNotNullPredicate node, ExpressionAnalysisContext context) {
            Symbol argument = node.getValue().accept(this, context);
            return allocateFunction(
                NotPredicate.NAME,
                ImmutableList.of(
                    allocateFunction(
                        io.crate.expression.predicate.IsNullPredicate.NAME,
                        ImmutableList.of(argument),
                        context)),
                context);
        }

        @Override
        protected Symbol visitSubscriptExpression(SubscriptExpression node, ExpressionAnalysisContext context) {
            SubscriptContext subscriptContext = new SubscriptContext();
            SubscriptValidator.validate(node, subscriptContext);
            QualifiedName qualifiedName = subscriptContext.qualifiedName();
            List<String> parts = subscriptContext.parts();

            if (qualifiedName == null) {
                Symbol base = node.base().accept(this, context);
                Symbol index = node.index().accept(this, context);
                return allocateFunction(SubscriptFunction.NAME, List.of(base, index), context);
            } else {
                // Ideally the above base+index + subscriptFunction case would be enough
                // But:
                // - We want to avoid subscript functions if possible (we've nested object values in a column store)
                // - In DDL statement we can't turn a `PRIMARY KEY o['x']` into a subscript either
                // - In DML statements we can have assignments: obj['x'] = 30
                // We should come up with a design that addresses those and remove the duct-tape logic below.

                Symbol name;
                try {
                    name = fieldProvider.resolveField(qualifiedName, parts, operation);
                } catch (ColumnUnknownException e) {
                    if (operation != Operation.READ) {
                        throw e;
                    }
                    try {
                        Symbol base = fieldProvider.resolveField(qualifiedName, List.of(), operation);
                        if (base instanceof Reference) {
                            throw e;
                        }
                        return allocateFunction(
                            SubscriptFunction.NAME,
                            List.of(
                                node.base().accept(this, context),
                                node.index().accept(this, context)
                            ),
                            context
                        );
                    } catch (ColumnUnknownException e2) {
                        throw e;
                    }
                }
                Expression idxExpression = subscriptContext.index();
                if (idxExpression != null) {
                    Symbol index = idxExpression.accept(this, context);
                    return allocateFunction(SubscriptFunction.NAME, List.of(name, index), context);
                }
                return name;
            }
        }

        @Override
        protected Symbol visitLogicalBinaryExpression(LogicalBinaryExpression node, ExpressionAnalysisContext context) {
            final String name;
            switch (node.getType()) {
                case AND:
                    name = AndOperator.NAME;
                    break;
                case OR:
                    name = OrOperator.NAME;
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported logical binary expression " + node.getType().name());
            }
            List<Symbol> arguments = ImmutableList.of(
                node.getLeft().accept(this, context),
                node.getRight().accept(this, context)
            );
            return allocateFunction(name, arguments, context);
        }

        @Override
        protected Symbol visitNotExpression(NotExpression node, ExpressionAnalysisContext context) {
            Symbol argument = node.getValue().accept(this, context);
            return allocateFunction(
                NotPredicate.NAME,
                ImmutableList.of(argument),
                context);
        }

        @Override
        protected Symbol visitComparisonExpression(ComparisonExpression node, ExpressionAnalysisContext context) {
            Symbol left = node.getLeft().accept(this, context);
            Symbol right = node.getRight().accept(this, context);

            Comparison comparison = new Comparison(functions, coordinatorTxnCtx, node.getType(), left, right);
            comparison.normalize(context);
            FunctionIdent ident = comparison.toFunctionIdent();
            return allocateFunction(ident.name(), comparison.arguments(), context);
        }

        @Override
        public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, ExpressionAnalysisContext context) {
            context.registerArrayChild(node.getRight());
            Symbol leftSymbol = node.getLeft().accept(this, context);
            Symbol arraySymbol = node.getRight().accept(this, context);
            ComparisonExpression.Type operationType = node.getType();
            final String operatorName;
            switch (node.quantifier()) {
                case ANY:
                    operatorName = AnyOperator.OPERATOR_PREFIX + operationType.getValue();
                    break;

                case ALL:
                    operatorName = AllOperator.OPERATOR_PREFIX + operationType.getValue();
                    break;

                default:
                    throw new IllegalStateException("Expected comparison quantifier ALL or ANY, got " + node.quantifier());
            }
            return allocateFunction(
                operatorName,
                ImmutableList.of(leftSymbol, arraySymbol),
                context);
        }

        @Override
        public Symbol visitArrayLikePredicate(ArrayLikePredicate node, ExpressionAnalysisContext context) {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("ESCAPE is not supported.");
            }
            Symbol arraySymbol = node.getValue().accept(this, context);
            Symbol leftSymbol = node.getPattern().accept(this, context);
            return allocateFunction(
                LikeOperators.arrayOperatorName(node.inverse(), node.ignoreCase()),
                ImmutableList.of(leftSymbol, arraySymbol),
                context);
        }

        @Override
        protected Symbol visitLikePredicate(LikePredicate node, ExpressionAnalysisContext context) {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("ESCAPE is not supported.");
            }
            Symbol expression = node.getValue().accept(this, context);
            Symbol pattern = node.getPattern().accept(this, context);
            return allocateFunction(
                LikeOperators.arrayOperatorName(node.ignoreCase()),
                ImmutableList.of(expression, pattern),
                context);
        }

        @Override
        protected Symbol visitIsNullPredicate(IsNullPredicate node, ExpressionAnalysisContext context) {
            Symbol value = node.getValue().accept(this, context);

            return allocateFunction(io.crate.expression.predicate.IsNullPredicate.NAME, ImmutableList.of(value), context);
        }

        @Override
        protected Symbol visitNegativeExpression(NegativeExpression node, ExpressionAnalysisContext context) {
            // `-1` in the AST is represented as NegativeExpression(LiteralInteger)
            // -> negate the inner value
            Symbol value = node.getValue().accept(this, context);
            var returnSymbol = NegateLiterals.negate(value);
            if (returnSymbol != null) {
                return returnSymbol;
            }
            return allocateFunction(NegateFunctions.NAME, List.of(value), context);
        }

        @Override
        protected Symbol visitArithmeticExpression(ArithmeticExpression node, ExpressionAnalysisContext context) {
            Symbol left = node.getLeft().accept(this, context);
            Symbol right = node.getRight().accept(this, context);

            return allocateFunction(
                node.getType().name().toLowerCase(Locale.ENGLISH),
                ImmutableList.of(left, right),
                context);
        }

        @Override
        public Symbol visitRecordSubscript(RecordSubscript recordSubscript,
                                           ExpressionAnalysisContext context) {
            // E.g.: SELECT (address).postcode FROM ...
            //  or   SELECT ((obj).x).y FROM ...
            //  or   SELECT ({x=10}).x FROM ...
            Expression base = recordSubscript.base();
            String field = recordSubscript.field();
            ArrayList<String> reversedPath = new ArrayList<>();
            reversedPath.add(field);
            while (base instanceof RecordSubscript) {
                RecordSubscript subscript = (RecordSubscript) base;
                base = subscript.base();
                reversedPath.add(subscript.field());
            }
            List<String> path = Lists.reverse(reversedPath);
            if (base instanceof QualifiedNameReference) {
                QualifiedName name = ((QualifiedNameReference) base).getName();
                try {
                    return fieldProvider.resolveField(name, path, operation);
                } catch (ColumnUnknownException e) {
                    var baseSymbol = fieldProvider.resolveField(name, List.of(), operation);
                    var subscriptFunction = SubscriptFunctions.tryCreateSubscript(baseSymbol, path);
                    if (subscriptFunction == null) {
                        throw e;
                    } else {
                        return subscriptFunction;
                    }
                }
            }
            Symbol baseSymbol = base.accept(this, context);
            var subscriptFunction = SubscriptFunctions.tryCreateSubscript(baseSymbol, path);
            if (subscriptFunction == null) {
                throw new UnsupportedOperationException(
                    "Unsupported expression `"
                    + ExpressionFormatter.formatStandaloneExpression(recordSubscript)
                    + "`, `"
                    + ExpressionFormatter.formatStandaloneExpression(base)
                    + "` should have type `object` or `record` but was `" + baseSymbol.valueType().getName() + '`');
            } else {
                return subscriptFunction;
            }
        }

        @Override
        protected Symbol visitQualifiedNameReference(QualifiedNameReference node, ExpressionAnalysisContext context) {
            try {
                return fieldProvider.resolveField(node.getName(), null, operation);
            } catch (ColumnUnknownException exception) {
                if (coordinatorTxnCtx.sessionContext().options().contains(Option.ALLOW_QUOTED_SUBSCRIPT)) {
                    String quotedSubscriptLiteral = getQuotedSubscriptLiteral(node.getName().toString());
                    if (quotedSubscriptLiteral != null) {
                        return SqlParser.createExpression(quotedSubscriptLiteral).accept(this, context);
                    } else {
                        throw exception;
                    }
                } else {
                    throw exception;
                }
            }
        }

        @Override
        protected Symbol visitBooleanLiteral(BooleanLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(node.getValue());
        }

        @Override
        protected Symbol visitStringLiteral(StringLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(node.getValue());
        }

        @Override
        protected Symbol visitEscapedCharStringLiteral(EscapedCharStringLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(node.getValue());
        }

        @Override
        protected Symbol visitDoubleLiteral(DoubleLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(node.getValue());
        }

        @Override
        protected Symbol visitLongLiteral(LongLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(node.getValue());
        }

        @Override
        protected Symbol visitNullLiteral(NullLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(UndefinedType.INSTANCE, null);
        }

        @Override
        public Symbol visitArrayLiteral(ArrayLiteral node, ExpressionAnalysisContext context) {
            List<Expression> values = node.values();
            if (values.isEmpty()) {
                return Literal.of(new ArrayType<>(UndefinedType.INSTANCE), List.of());
            } else {
                List<Symbol> arguments = new ArrayList<>(values.size());
                for (Expression value : values) {
                    arguments.add(value.accept(this, context));
                }
                return allocateFunction(ArrayFunction.NAME, arguments, context);
            }
        }

        @Override
        public Symbol visitObjectLiteral(ObjectLiteral node, ExpressionAnalysisContext context) {
            Map<String, Expression> values = node.values();
            if (values.isEmpty()) {
                return Literal.EMPTY_OBJECT;
            }
            List<Symbol> arguments = new ArrayList<>(values.size() * 2);
            for (Map.Entry<String, Expression> entry : values.entrySet()) {
                arguments.add(Literal.of(entry.getKey()));
                arguments.add(entry.getValue().accept(this, context));
            }
            return allocateFunction(
                MapFunction.NAME,
                arguments,
                context);
        }

        private final Map<IntervalLiteral.IntervalField, IntervalParser.Precision> INTERVAL_FIELDS =
            Map.of(
                YEAR, IntervalParser.Precision.YEAR,
                MONTH, IntervalParser.Precision.MONTH,
                DAY, IntervalParser.Precision.DAY,
                HOUR, IntervalParser.Precision.HOUR,
                MINUTE, IntervalParser.Precision.MINUTE,
                SECOND, IntervalParser.Precision.SECOND
            );

        @Override
        public Symbol visitIntervalLiteral(IntervalLiteral node, ExpressionAnalysisContext context) {
            String value = node.getValue();

            IntervalParser.Precision start = INTERVAL_FIELDS.get(node.getStartField());
            IntervalParser.Precision end = node.getEndField() == null ? null : INTERVAL_FIELDS.get(node.getEndField());

            Period period = IntervalParser.apply(value, start, end);

            if (node.getSign() == IntervalLiteral.Sign.MINUS) {
                period = period.negated();
            }
            return Literal.newInterval(period);
        }

        @Override
        public Symbol visitParameterExpression(ParameterExpression node, ExpressionAnalysisContext context) {
            return paramTypeHints.apply(node);
        }

        @Override
        protected Symbol visitBetweenPredicate(BetweenPredicate node, ExpressionAnalysisContext context) {
            // <value> between <min> and <max>
            // -> <value> >= <min> and <value> <= max
            Symbol value = node.getValue().accept(this, context);
            Symbol min = node.getMin().accept(this, context);
            Symbol max = node.getMax().accept(this, context);

            Comparison gte = new Comparison(functions, coordinatorTxnCtx, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, value, min);
            FunctionIdent gteIdent = gte.normalize(context).toFunctionIdent();
            Symbol gteFunc = allocateFunction(
                gteIdent.name(),
                gte.arguments(),
                context);

            Comparison lte = new Comparison(functions, coordinatorTxnCtx, ComparisonExpression.Type.LESS_THAN_OR_EQUAL, value, max);
            FunctionIdent lteIdent = lte.normalize(context).toFunctionIdent();
            Symbol lteFunc = allocateFunction(
                lteIdent.name(),
                lte.arguments(),
                context);

            return allocateFunction(AndOperator.NAME, ImmutableList.of(gteFunc, lteFunc), context);
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate node, ExpressionAnalysisContext context) {
            Map<Symbol, Symbol> identBoostMap = new HashMap<>(node.idents().size());
            DataType<?> columnType = null;
            HashSet<RelationName> relationsInColumns = new HashSet<>();
            for (MatchPredicateColumnIdent ident : node.idents()) {
                Symbol column = ident.columnIdent().accept(this, context);
                if (columnType == null) {
                    columnType = column.valueType();
                }
                Preconditions.checkArgument(
                    column instanceof ScopedSymbol || column instanceof Reference,
                    Symbols.format("can only MATCH on columns, not on %s", column));
                Symbol boost = ident.boost().accept(this, context);
                identBoostMap.put(column, boost);
                if (column instanceof ScopedSymbol) {
                    relationsInColumns.add(((ScopedSymbol) column).relation());
                }
            }
            if (relationsInColumns.size() > 1) {
                throw new IllegalArgumentException("Cannot use MATCH predicates on columns of 2 different relations");
            }
            assert columnType != null : "columnType must not be null";
            verifyTypesForMatch(identBoostMap.keySet(), columnType);

            Symbol queryTerm = node.value().accept(this, context).cast(columnType);
            String matchType = io.crate.expression.predicate.MatchPredicate.getMatchType(node.matchType(), columnType);

            List<Symbol> mapArgs = new ArrayList<>(node.properties().size() * 2);
            for (Map.Entry<String, Expression> e : node.properties().properties().entrySet()) {
                mapArgs.add(Literal.of(e.getKey()));
                mapArgs.add(e.getValue().accept(this, context));
            }
            Symbol options = mapArgs.isEmpty() ? Literal.of(Map.of()) : allocateFunction(MapFunction.NAME, mapArgs, context);
            return new io.crate.expression.symbol.MatchPredicate(identBoostMap, queryTerm, matchType, options);
        }

        @Override
        protected Symbol visitSubqueryExpression(SubqueryExpression node, ExpressionAnalysisContext context) {
            if (subQueryAnalyzer == null) {
                throw new UnsupportedOperationException("Subquery not supported in this statement");
            }
            /* note: This does not support analysis columns in the subquery which belong to the parent relation
             * this would require {@link StatementAnalysisContext#startRelation} to somehow inherit the parent context
             */
            AnalyzedRelation relation = subQueryAnalyzer.analyze(node.getQuery());
            List<Symbol> fields = relation.outputs();
            if (fields.size() > 1) {
                throw new UnsupportedOperationException("Subqueries with more than 1 column are not supported.");
            }
            /*
             * The SelectSymbol should actually have a RowType as it is a row-expression.
             *
             * But there are no other row-expressions yet. In addition the cast functions and operators don't work with
             * row types (yet).
             *
             * However, we support a single column RowType through the ArrayType.
             */
            DataType<?> innerType = fields.get(0).valueType();
            ArrayType<?> dataType = new ArrayType<>(innerType);
            final SelectSymbol.ResultType resultType;
            if (context.isArrayChild(node)) {
                resultType = SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES;
            } else {
                resultType = SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;
            }
            return new SelectSymbol(relation, dataType, resultType);
        }
    }

    private Symbol allocateBuiltinOrUdfFunction(String schema,
                                                String functionName,
                                                List<Symbol> arguments,
                                                Symbol filter,
                                                WindowDefinition windowDefinition,
                                                ExpressionAnalysisContext context) {
        return allocateBuiltinOrUdfFunction(
            schema, functionName, arguments, filter, context, functions, windowDefinition, coordinatorTxnCtx);
    }

    private Symbol allocateFunction(String functionName,
                                    List<Symbol> arguments,
                                    ExpressionAnalysisContext context) {
        return allocateFunction(functionName, arguments, null, context, functions, coordinatorTxnCtx);
    }

    public static Symbol allocateFunction(String functionName,
                                          List<Symbol> arguments,
                                          Symbol filter,
                                          ExpressionAnalysisContext context,
                                          Functions functions,
                                          CoordinatorTxnCtx coordinatorTxnCtx) {
        return allocateBuiltinOrUdfFunction(
            null, functionName, arguments, filter, context, functions, null, coordinatorTxnCtx);
    }

    /**
     * Creates a function symbol and tries to normalize the new function's
     * {@link FunctionImplementation} iff only Literals are supplied as arguments.
     * This folds any constant expressions like '1 + 1' => '2'.
     * @param schema The schema for udf functions
     * @param functionName The function name of the new function.
     * @param arguments The arguments to provide to the {@link Function}.
     * @param filter The filter clause to filter {@link Function}'s input values.
     * @param context Context holding the state for the current translation.
     * @param functions The {@link Functions} to normalize constant expressions.
     * @param windowDefinition The definition of the window the allocated function will be executed against.
     * @param coordinatorTxnCtx {@link CoordinatorTxnCtx} for this transaction.
     * @return The supplied {@link Function} or a {@link Literal} in case of constant folding.
     */
    private static Symbol allocateBuiltinOrUdfFunction(@Nullable String schema,
                                                       String functionName,
                                                       List<Symbol> arguments,
                                                       @Nullable Symbol filter,
                                                       ExpressionAnalysisContext context,
                                                       Functions functions,
                                                       @Nullable WindowDefinition windowDefinition,
                                                       CoordinatorTxnCtx coordinatorTxnCtx) {
        FunctionImplementation funcImpl = functions.get(
            schema,
            functionName,
            arguments,
            coordinatorTxnCtx.sessionContext().searchPath());

        FunctionInfo functionInfo = funcImpl.info();
        Signature signature = funcImpl.signature();
        List<Symbol> castArguments = cast(arguments, functionInfo.ident().argumentTypes());
        Function newFunction;
        if (windowDefinition == null) {
            if (functionInfo.type() == FunctionInfo.Type.AGGREGATE) {
                context.indicateAggregates();
            } else if (filter != null) {
                throw new UnsupportedOperationException(
                    "Only aggregate functions allow a FILTER clause");
            }
            newFunction = new Function(functionInfo, signature, castArguments, filter);
        } else {
            if (functionInfo.type() != FunctionInfo.Type.WINDOW && functionInfo.type() != FunctionInfo.Type.AGGREGATE) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "OVER clause was specified, but %s is neither a window nor an aggregate function.",
                    functionName));
            }
            newFunction = new WindowFunction(
                functionInfo,
                signature,
                castArguments,
                filter,
                windowDefinition);
        }
        if (context.isEagerNormalizationAllowed() && functionInfo.isDeterministic()) {
            return funcImpl.normalizeSymbol(newFunction, coordinatorTxnCtx);
        }
        return newFunction;
    }

    private static void verifyTypesForMatch(Iterable<? extends Symbol> columns, DataType<?> columnType) {
        Preconditions.checkArgument(
            io.crate.expression.predicate.MatchPredicate.SUPPORTED_TYPES.contains(columnType),
            String.format(Locale.ENGLISH, "Can only use MATCH on columns of type STRING or GEO_SHAPE, not on '%s'", columnType));
        for (Symbol column : columns) {
            if (!column.valueType().equals(columnType)) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "All columns within a match predicate must be of the same type. Found %s and %s",
                    columnType, column.valueType()));
            }
        }
    }

    private static void ensureResultTypesMatch(Collection<? extends Symbol> results) {
        HashSet<DataType> resultTypes = new HashSet<>();
        for (Symbol result : results) {
            resultTypes.add(result.valueType());
        }
        if (resultTypes.size() == 2 && !resultTypes.contains(DataTypes.UNDEFINED) || resultTypes.size() > 2) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Data types of all result expressions of a CASE statement must be equal, found: %s",
                resultTypes));
        }
    }

    private static class Comparison {

        private final Functions functions;
        private final CoordinatorTxnCtx coordinatorTxnCtx;
        private ComparisonExpression.Type comparisonExpressionType;
        private Symbol left;
        private Symbol right;
        private String operatorName;
        private FunctionIdent functionIdent;

        private Comparison(Functions functions,
                           CoordinatorTxnCtx coordinatorTxnCtx,
                           ComparisonExpression.Type comparisonExpressionType,
                           Symbol left,
                           Symbol right) {
            this.functions = functions;
            this.coordinatorTxnCtx = coordinatorTxnCtx;
            this.operatorName = Operator.PREFIX + comparisonExpressionType.getValue();
            this.comparisonExpressionType = comparisonExpressionType;
            this.left = left;
            this.right = right;
        }

        Comparison normalize(ExpressionAnalysisContext context) {
            swapIfNecessary();
            rewriteNegatingOperators(context);
            return this;
        }

        /**
         * swaps the comparison so that references and fields are on the left side.
         * e.g.:
         * eq(2, name)  becomes  eq(name, 2)
         */
        private void swapIfNecessary() {
            if ((!(right instanceof Reference || right instanceof ScopedSymbol)
                || left instanceof Reference || left instanceof ScopedSymbol)
                && left.valueType().id() != DataTypes.UNDEFINED.id()) {
                return;
            }
            ComparisonExpression.Type type = SWAP_OPERATOR_TABLE.get(comparisonExpressionType);
            if (type != null) {
                comparisonExpressionType = type;
                operatorName = Operator.PREFIX + type.getValue();
            }
            Symbol tmp = left;
            left = right;
            right = tmp;
        }

        /**
         * rewrite   exp1 != exp2  to not(eq(exp1, exp2))
         * and       exp1 !~ exp2  to not(~(exp1, exp2))
         * does nothing if operator != not equals
         */
        private void rewriteNegatingOperators(ExpressionAnalysisContext context) {
            final String opName;
            switch (comparisonExpressionType) {
                case NOT_EQUAL:
                    opName = EqOperator.NAME;
                    break;
                case REGEX_NO_MATCH:
                    opName = RegexpMatchOperator.NAME;
                    break;
                case REGEX_NO_MATCH_CI:
                    opName = RegexpMatchCaseInsensitiveOperator.NAME;
                    break;

                default:
                    return; // non-negating comparison
            }
            left = allocateFunction(
                opName,
                List.of(left, right),
                null,
                context,
                functions,
                coordinatorTxnCtx);
            right = null;
            functionIdent = NotPredicate.INFO.ident();
            operatorName = NotPredicate.NAME;
        }

        FunctionIdent toFunctionIdent() {
            if (functionIdent == null) {
                return new FunctionIdent(
                    operatorName,
                    Arrays.asList(left.valueType(), right.valueType()));
            }
            return functionIdent;
        }

        List<Symbol> arguments() {
            if (right == null) {
                // this is the case if the comparison has been rewritten to not(eq(exp1, exp2))
                return ImmutableList.of(left);
            }
            return ImmutableList.of(left, right);
        }
    }
}
