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

package io.crate.analyze.expressions;

import static io.crate.sql.tree.IntervalLiteral.IntervalField.DAY;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.MINUTE;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.MONTH;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.SECOND;
import static io.crate.sql.tree.IntervalLiteral.IntervalField.YEAR;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.Period;

import io.crate.analyze.DataTypeAnalyzer;
import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.NegateLiterals;
import io.crate.analyze.OrderBy;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.SubscriptContext;
import io.crate.analyze.SubscriptVisitor;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.OrderyByAnalyzer;
import io.crate.analyze.relations.SelectListFieldProvider;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.common.collections.Lists;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.ExistsOperator;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.expression.operator.RegexpMatchOperator;
import io.crate.expression.operator.all.AllOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.ArraySliceFunction;
import io.crate.expression.scalar.ArrayUnnestFunction;
import io.crate.expression.scalar.CurrentDateFunction;
import io.crate.expression.scalar.ExtractFunctions;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.SubscriptFunctions;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.scalar.arithmetic.NegateFunctions;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.scalar.conditional.CaseFunction;
import io.crate.expression.scalar.conditional.IfFunction;
import io.crate.expression.scalar.timestamp.CurrentTimeFunction;
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
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.table.Operation;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.Identifiers;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArithmeticExpression;
import io.crate.sql.tree.ArrayComparison;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.ArraySliceExpression;
import io.crate.sql.tree.ArraySubQueryExpression;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BetweenPredicate;
import io.crate.sql.tree.BitString;
import io.crate.sql.tree.BitwiseExpression;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.ExistsPredicate;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.IfExpression;
import io.crate.sql.tree.InListExpression;
import io.crate.sql.tree.InPredicate;
import io.crate.sql.tree.IntegerLiteral;
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
import io.crate.sql.tree.NumericLiteral;
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
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.UndefinedType;

/**
 * <p>This Analyzer can be used to convert Expression from the SQL AST into symbols.</p>
 * <p>
 * <p>
 * In order to resolve QualifiedName or SubscriptExpressions it will use the fieldResolver given in the constructor and
 * generate a relationOutput for the matching Relation.
 * </p>
 */
public class ExpressionAnalyzer {

    public static final Map<ComparisonExpression.Type, ComparisonExpression.Type> SWAP_OPERATOR_TABLE = Map.of(
        ComparisonExpression.Type.GREATER_THAN, ComparisonExpression.Type.LESS_THAN,
        ComparisonExpression.Type.LESS_THAN, ComparisonExpression.Type.GREATER_THAN,
        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
        ComparisonExpression.Type.LESS_THAN_OR_EQUAL, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL
    );

    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final ParamTypeHints paramTypeHints;
    private final FieldProvider<?> fieldProvider;

    @Nullable
    private final SubqueryAnalyzer subQueryAnalyzer;
    private final NodeContext nodeCtx;
    private final InnerExpressionAnalyzer innerAnalyzer;
    private final Operation operation;

    public ExpressionAnalyzer(CoordinatorTxnCtx coordinatorTxnCtx,
                              NodeContext nodeCtx,
                              ParamTypeHints paramTypeHints,
                              FieldProvider<?> fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer) {
        this(coordinatorTxnCtx, nodeCtx, paramTypeHints, fieldProvider, subQueryAnalyzer, Operation.READ);
    }

    public ExpressionAnalyzer(CoordinatorTxnCtx coordinatorTxnCtx,
                              NodeContext nodeCtx,
                              ParamTypeHints paramTypeHints,
                              FieldProvider<?> fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer,
                              Operation operation) {
        this.nodeCtx = nodeCtx;
        this.coordinatorTxnCtx = coordinatorTxnCtx;
        this.paramTypeHints = paramTypeHints;
        this.fieldProvider = fieldProvider;
        this.subQueryAnalyzer = subQueryAnalyzer;
        this.innerAnalyzer = new InnerExpressionAnalyzer();
        this.operation = operation;
    }

    /**
     * Derive a new ExpressionAnalyzer that tries to resolve expressions by
     * looking at the select-list before falling back to regular column resolving logic.
     */
    public ExpressionAnalyzer referenceSelect(SelectAnalysis selectAnalysis) {
        var selectListFieldProvider = new SelectListFieldProvider(selectAnalysis, fieldProvider);
        return new ExpressionAnalyzer(
            coordinatorTxnCtx,
            nodeCtx,
            paramTypeHints,
            selectListFieldProvider,
            subQueryAnalyzer,
            operation
        );
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
        var symbol = expression.accept(innerAnalyzer, expressionAnalysisContext);
        var normalizer = EvaluatingNormalizer.functionOnlyNormalizer(
            nodeCtx,
            f -> expressionAnalysisContext.isEagerNormalizationAllowed() && f.signature().isDeterministic()
        );
        return normalizer.normalize(symbol, coordinatorTxnCtx);
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
                coordinatorTxnCtx,
                nodeCtx);

            // define the outer function which contains the inner function as argument.
            String nodeName = "collection_" + name;
            List<Symbol> outerArguments = List.of(collectSetFunction);
            try {
                return allocateBuiltinOrUdfFunction(
                    schema, nodeName, outerArguments, null, node.ignoreNulls(), windowDefinition, context);
            } catch (UnsupportedOperationException ex) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "unknown function %s(DISTINCT %s)", name, arguments.get(0).valueType()), ex);
            }
        } else {
            return allocateBuiltinOrUdfFunction(schema, name, arguments, filter, node.ignoreNulls(), windowDefinition, context);
        }
    }

    @Nullable
    private WindowDefinition getWindowDefinition(Optional<Window> maybeWindow,
                                                 ExpressionAnalysisContext context) {
        if (maybeWindow.isEmpty()) {
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
    private Window resolveWindowRef(@NotNull String name, Map<String, Window> windows) {
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
    private static List<Symbol> cast(List<Symbol> symbolsToCast, List<DataType<?>> targetTypes) {
        if (symbolsToCast.size() != targetTypes.size()) {
            throw new IllegalStateException("Given symbol list has to match the target type list.");
        }
        int size = symbolsToCast.size();
        List<Symbol> castList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            var symbolToCast = symbolsToCast.get(i);
            var targetType = targetTypes.get(i);
            if (targetType.id() == UndefinedType.ID) {
                castList.add(symbolToCast);
            } else {
                castList.add(symbolToCast.cast(targetType));
            }
        }
        return castList;
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
            String funcName = CurrentTimestampFunction.NAME;
            switch (node.getType()) {
                case TIMESTAMP:
                    break;

                case TIME:
                    funcName = CurrentTimeFunction.NAME;
                    break;

                case DATE:
                    funcName = CurrentDateFunction.NAME;
                    break;

                default:
                    visitExpression(node, context);
            }
            Optional<Integer> p = node.getPrecision();
            return allocateFunction(
                funcName,
                p.isPresent() ? List.of(Literal.ofUnchecked(DataTypes.INTEGER, p.get())) : List.of(),
                context);
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
            var whenClauses = node.getWhenClauses();
            // For each whenClause we create two values `boolean, T` and two more for the default value.
            var arguments = new ArrayList<Symbol>(whenClauses.size() * 2 + 2);

            var defaultValue = node.getDefaultValue();
            // See {@link CaseFunction} for why true
            arguments.add(BooleanLiteral.TRUE_LITERAL.accept(this, context));

            if (defaultValue == null) {
                arguments.add(convert(NullLiteral.INSTANCE, context));
            } else {
                arguments.add(convert(defaultValue, context));
            }

            for (WhenClause whenClause : whenClauses) {
                arguments.add(allocateFunction(
                    EqOperator.NAME,
                    List.of(convert(node.getOperand(), context), convert(whenClause.getOperand(), context)),
                    context)
                );
                arguments.add(whenClause.getResult().accept(this, context));
            }
            ensureResultTypesMatch(arguments);
            return allocateFunction(CaseFunction.NAME, arguments, context);
        }

        @Override
        protected Symbol visitSearchedCaseExpression(SearchedCaseExpression node, ExpressionAnalysisContext context) {
            var whenClauses = node.getWhenClauses();
            // For each whenClause we create two values `boolean, T` and two more for the default value.
            var arguments = new ArrayList<Symbol>(whenClauses.size() * 2 + 2);

            var defaultValue = node.getDefaultValue();
            // See {@link CaseFunction} for why true
            arguments.add(BooleanLiteral.TRUE_LITERAL.accept(this, context));

            if (defaultValue == null) {
                arguments.add(NullLiteral.INSTANCE.accept(this, context));
            } else {
                arguments.add(convert(defaultValue, context));
            }

            for (var whenClause : whenClauses) {
                arguments.add(whenClause.getOperand().accept(innerAnalyzer, context));
                arguments.add(whenClause.getResult().accept(innerAnalyzer, context));
            }
            ensureResultTypesMatch(arguments);
            return allocateFunction(CaseFunction.NAME, arguments, context);
        }

        @Override
        protected Symbol visitCast(Cast node, ExpressionAnalysisContext context) {
            Symbol expression = node.getExpression().accept(this, context);
            DataType<?> returnType = DataTypeAnalyzer.convert(node.getType());
            DataType<?> valueType = expression.valueType();
            DataType<?> unnestedReturnType = ArrayType.unnest(returnType);
            DataType<?> unnestedValueType = ArrayType.unnest(valueType);

            if (unnestedReturnType.id() == ObjectType.ID
                && unnestedValueType.id() == ObjectType.ID
                && unnestedReturnType.columnPolicy() != ColumnPolicy.STRICT) {
                returnType = DataTypes.merge(valueType, returnType, returnType.columnPolicy());
            }
            if (node.isIntegerOnly() && !returnType.isConvertableTo(DataTypes.INTEGER, false)) {
                throw new IllegalArgumentException("Cannot cast to a datatype that is not convertible to `integer`");
            }
            return expression
                .cast(
                    returnType,
                    CastMode.EXPLICIT
                );
        }

        @Override
        protected Symbol visitTryCast(TryCast node, ExpressionAnalysisContext context) {
            DataType<?> returnType = DataTypeAnalyzer.convert(node.getType());
            if (node.isIntegerOnly() && !returnType.isConvertableTo(DataTypes.INTEGER, false)) {
                throw new IllegalArgumentException("Cannot cast to a datatype that is not convertible to `integer`");
            }
            try {
                return node.getExpression()
                    .accept(this, context)
                    .cast(
                        returnType,
                        CastMode.EXPLICIT,
                        CastMode.TRY
                    );
            } catch (ConversionException e) {
                return Literal.NULL;
            }
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
            if (valueList instanceof InListExpression inListExpression) {
                List<Expression> expressions = inListExpression.getValues();
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
                List.of(
                    allocateFunction(
                        io.crate.expression.predicate.IsNullPredicate.NAME,
                        List.of(argument),
                        context)),
                context);
        }

        @Override
        protected Symbol visitExists(ExistsPredicate node, ExpressionAnalysisContext context) {
            if (subQueryAnalyzer == null) {
                throw new UnsupportedOperationException("Subquery not supported in this statement");
            }
            var relation = subQueryAnalyzer.analyze(node.getSubquery());
            List<Symbol> fields = relation.outputs();
            if (fields.size() > 1) {
                throw new UnsupportedOperationException("Subqueries with more than 1 column are not supported");
            }
            DataType<?> innerType = fields.getFirst().valueType();
            SelectSymbol selectSymbol = new SelectSymbol(
                relation,
                new ArrayType<>(innerType),
                SelectSymbol.ResultType.SINGLE_COLUMN_EXISTS,
                false
            );
            return allocateFunction(ExistsOperator.NAME, List.of(selectSymbol), context);
        }

        @Override
        protected Symbol visitSubscriptExpression(SubscriptExpression node, ExpressionAnalysisContext context) {
            SubscriptContext subscriptContext = SubscriptVisitor.visit(node);

            if (subscriptContext.hasExpression()) {
                // The left hand side of the expression isn't a column, it's something like a
                // static array, function or a cast, so we recurse into it.
                Symbol base = node.base().accept(this, context);
                Symbol index = node.index().accept(this, context);
                // If the index is a string, we can try to optimize the subscript function
                // to avoid the function call and directly access the object key.
                if (index.valueType() == DataTypes.STRING && subscriptContext.parts().isEmpty() == false) {
                    // Only the last path must be used (the index string value). The visitor will collect all parts of
                    // the symbol tree, while with expression, only the path on the expression itself must be considered.
                    Function optimizedSubscript = optimizedSubscriptFunction(base, index, List.of(subscriptContext.parts().getLast()), context, null);
                    if (optimizedSubscript != null) {
                        return optimizedSubscript;
                    }
                }
                return allocateFunction(SubscriptFunction.NAME, List.of(base, index), context);
            }

            // Detect and process partial quoted subscript expression
            QualifiedName qualifiedName = subscriptContext.qualifiedName();
            var columnName = qualifiedName.getSuffix();
            var maybeQuotedSubscript = detectAndGenerateSubscriptExpressions(columnName);
            if (maybeQuotedSubscript != null) {
                return visitSubscriptExpression(new SubscriptExpression(maybeQuotedSubscript, node.index()), context);
            }

            // Ideally the above base+index + subscriptFunction case would be enough
            // But:
            // - We want to avoid subscript functions if possible (we've nested object values in a column store)
            // - In DDL statement we can't turn a `PRIMARY KEY o['x']` into a subscript either
            // - In DML statements we can have assignments: obj['x'] = 30
            // - We want a good error message if the subscript is invalid including the column name and relation. This
            //   is not possible with the subscript function as it misses all these information.
            // We should come up with a design that addresses those and remove the duct-tape logic below.

            Symbol ref;
            List<String> parts = subscriptContext.parts();
            try {
                ref = fieldProvider.resolveField(qualifiedName, parts, operation, context.errorOnUnknownObjectKey());
            } catch (ColumnUnknownException e) {
                return resolveUnindexedSubscriptExpression(node, context, qualifiedName, parts, e);
            }

            // If there are any array subscripts, recursively wrap the resolved expression in an
            // array subscript function for each nested array dereference.
            for (Expression idx : subscriptContext.index()) {
                Symbol index = idx.accept(this, context);
                ref = allocateFunction(SubscriptFunction.NAME, List.of(ref, index), context);
            }
            return ref;
        }

        // If a subscript expression doesn't resolve to an indexed field, try instead
        // to resolve the base field and use a subscript function to extract the values
        private Symbol resolveUnindexedSubscriptExpression(
            SubscriptExpression node,
            ExpressionAnalysisContext context,
            QualifiedName qualifiedName,
            List<String> parts,
            ColumnUnknownException e) {
            if (operation != Operation.READ || parts.isEmpty()) {
                throw e;
            }

            Symbol parent = null;
            List<String> childParts = parts;
            for (int i = parts.size() - 1; i >= 0; i--) {
                List<String> parentParts = parts.subList(0, i);
                try {
                    parent = fieldProvider.resolveField(qualifiedName, parentParts, operation, context.errorOnUnknownObjectKey());
                    childParts = parts.subList(i, parts.size());
                    break;
                } catch (ColumnUnknownException e2) {
                    if (i == 0) {
                        throw e;
                    }
                }
            }
            assert parent != null : "Parent symbol must not be null without throwing an exception";

            try {
                Symbol index = node.index().accept(this, context);
                Function optimizedSubscript = optimizedSubscriptFunction(parent, index, childParts, context, e);
                if (optimizedSubscript != null) {
                    return optimizedSubscript;
                }

                return allocateFunction(
                    SubscriptFunction.NAME,
                    List.of(parent, index),
                    context
                );
            } catch (ColumnUnknownException e2) {
                throw e;
            }
        }

        /**
         * Tries to detect the subscript return type based on the given (typed) base type (object or arrayOfObjects).
         *
         * @return Subscript function with concrete return type or NULL if it cannot be detected.
         * @throws ColumnUnknownException if the wanted column cannot be found and the parent policy condition requires
         *         an error to be thrown. Only thrown if the exception is passed in as an argument. Otherwise, it falls
         *         through and relies on the SubscriptFunction to throw the error.
         */
        @Nullable
        private Function optimizedSubscriptFunction(Symbol base,
                                                    Symbol index,
                                                    List<String> path,
                                                    ExpressionAnalysisContext context,
                                                    @Nullable ColumnUnknownException e) {
            if (path.isEmpty()) {
                return null;
            }
            DataType<?> baseType = base.valueType();
            DataType<?> innerType = DataTypes.innerType(baseType, path);
            if (innerType != null && innerType != UndefinedType.INSTANCE && !DataTypes.isArrayOfNulls(innerType)) {
                Signature signature = SubscriptFunction.SIGNATURE_OBJECT;
                if (baseType instanceof ArrayType<?>) {
                    signature = SubscriptFunction.SIGNATURE_ARRAY_OF_OBJECTS;
                }
                return new Function(
                    signature,
                    List.of(base, index),
                    innerType
                );
            }
            DataType<?> currentType = baseType;
            ColumnPolicy parentPolicy = baseType.columnPolicy();
            for (String p : path) {
                if (ArrayType.unnest(currentType) instanceof ObjectType objectType) {
                    parentPolicy = objectType.columnPolicy();
                    currentType = objectType.innerType(p);
                }
            }
            if (parentPolicy == ColumnPolicy.STRICT ||
                (parentPolicy == ColumnPolicy.DYNAMIC && context.errorOnUnknownObjectKey())) {
                if (e != null) {
                    throw e;
                }
                throw ColumnUnknownException.forSubscript(base, path.getLast());
            }
            return null;
        }

        @Override
        protected Symbol visitArraySliceExpression(ArraySliceExpression node,
                                                   ExpressionAnalysisContext context) {

            Symbol base = node.getBase().accept(this, context);
            Symbol from = node.getFrom().map(f -> f.accept(this, context)).orElse(Literal.NULL);
            Symbol to = node.getTo().map(f -> f.accept(this, context)).orElse(Literal.NULL);
            return allocateFunction(ArraySliceFunction.NAME, List.of(base, from, to), context);
        }

        @Override
        protected Symbol visitLogicalBinaryExpression(LogicalBinaryExpression node, ExpressionAnalysisContext context) {
            final String name = switch (node.getType()) {
                case AND -> AndOperator.NAME;
                case OR -> OrOperator.NAME;
            };
            List<Symbol> arguments = List.of(
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
                List.of(argument),
                context);
        }

        @Override
        protected Symbol visitComparisonExpression(ComparisonExpression node, ExpressionAnalysisContext context) {
            Symbol left = node.getLeft().accept(this, context);
            Symbol right = node.getRight().accept(this, context);

            Comparison comparison = new Comparison(coordinatorTxnCtx, nodeCtx, node.getType(), left, right);
            comparison.normalize(context);
            return allocateFunction(comparison.operatorName, comparison.arguments(), context);
        }

        @Override
        public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, ExpressionAnalysisContext context) {
            context.registerArrayChild(node.getRight());
            context.parentIsOrderSensitive(false);
            Symbol leftSymbol = node.getLeft().accept(this, context);
            Symbol arraySymbol = node.getRight().accept(this, context);

            // Automatically unnest right side to required number of dimensions
            // E.g. `1 = ANY([ [1, 2], [3, 4] ])` behaves like `1 = ANY([1, 2, 3, 4])`
            // This matches PostgreSQL semantics.
            // If negative, the function lookup further below will fail with an appropriate error message
            int leftDimensions = ArrayType.dimensions(leftSymbol.valueType());
            int rightDimensions = ArrayType.dimensions(arraySymbol.valueType());
            int diff = rightDimensions - leftDimensions;
            arraySymbol = ArrayUnnestFunction.unnest(arraySymbol, diff - 1);

            context.parentIsOrderSensitive(true);
            ComparisonExpression.Type operationType = node.getType();
            final String operatorName = switch (node.quantifier()) {
                case ANY -> AnyOperator.OPERATOR_PREFIX + operationType.getValue();
                case ALL -> AllOperator.OPERATOR_PREFIX + operationType.getValue();
            };
            return allocateFunction(
                operatorName,
                List.of(leftSymbol, arraySymbol),
                context);
        }

        @Override
        public Symbol visitArrayLikePredicate(ArrayLikePredicate node, ExpressionAnalysisContext context) {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("ESCAPE is not supported.");
            }
            Symbol value = node.getValue().accept(this, context);
            Symbol pattern = node.getPattern().accept(this, context);
            int valueDimensions = ArrayType.dimensions(value.valueType());
            int patternDimensions = ArrayType.dimensions(pattern.valueType());
            int diff = valueDimensions - patternDimensions;
            value = ArrayUnnestFunction.unnest(value, diff - 1);
            return allocateFunction(
                LikeOperators.arrayOperatorName(node.quantifier(), node.inverse(), node.ignoreCase()),
                List.of(pattern, value),
                context);
        }

        @Override
        protected Symbol visitLikePredicate(LikePredicate node, ExpressionAnalysisContext context) {
            Symbol expression = node.getValue().accept(this, context);
            Symbol pattern = node.getPattern().accept(this, context);
            if (node.getEscape() != null) {
                Symbol escape = node.getEscape().accept(this, context);
                return allocateFunction(
                    LikeOperators.likeOperatorName(node.ignoreCase()),
                    List.of(expression, pattern, escape),
                    context
                );
            } else {
                return allocateFunction(
                    LikeOperators.likeOperatorName(node.ignoreCase()),
                    List.of(expression, pattern),
                    context
                );
            }

        }

        @Override
        protected Symbol visitIsNullPredicate(IsNullPredicate node, ExpressionAnalysisContext context) {
            Symbol value = node.getValue().accept(this, context);

            return allocateFunction(io.crate.expression.predicate.IsNullPredicate.NAME, List.of(value), context);
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
                List.of(left, right),
                context);
        }

        @Override
        protected Symbol visitBitwiseExpression(BitwiseExpression node, ExpressionAnalysisContext context) {
            Symbol left = node.getLeft().accept(this, context);
            Symbol right = node.getRight().accept(this, context);

            return allocateFunction(
                node.getType().name().toLowerCase(Locale.ENGLISH),
                List.of(left, right),
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
            while (base instanceof RecordSubscript subscript) {
                base = subscript.base();
                reversedPath.add(subscript.field());
            }
            List<String> path = Lists.reverse(reversedPath);
            if (base instanceof QualifiedNameReference qnr) {
                QualifiedName name = qnr.getName();
                try {
                    return fieldProvider.resolveField(name, path, operation, context.errorOnUnknownObjectKey());
                } catch (ColumnUnknownException e) {
                    var baseSymbol = fieldProvider.resolveField(name,List.of(), operation, context.errorOnUnknownObjectKey());
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
            var qualifiedName = node.getName();
            var parts = qualifiedName.getParts();
            var columnName = parts.getLast();

            // Detect and process quoted subscript expressions
            var maybeQuotedSubscript = detectAndGenerateSubscriptExpressions(columnName);
            if (maybeQuotedSubscript != null) {
                return visitSubscriptExpression(maybeQuotedSubscript, context);
            }

            return fieldProvider.resolveField(node.getName(), null, operation, context.errorOnUnknownObjectKey());
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
        public Symbol visitBitString(BitString bitString, ExpressionAnalysisContext context) {
            BitStringType bitStringType = new BitStringType(bitString.length());
            return Literal.of(bitStringType, bitString);
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
        protected Symbol visitIntegerLiteral(IntegerLiteral node, ExpressionAnalysisContext context) {
            return Literal.of(node.getValue());
        }

        @Override
        public Symbol visitNumericLiteral(NumericLiteral numericLiteral, ExpressionAnalysisContext context) {
            BigDecimal value = numericLiteral.value();
            NumericType numericType = new NumericType(value.precision(), value.scale());
            return Literal.of(numericType, value);
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
            var returnTypeBuilder = ObjectType.of(ColumnPolicy.DYNAMIC);
            for (Map.Entry<String, Expression> entry : values.entrySet()) {
                var val = entry.getValue().accept(this, context);
                arguments.add(Literal.of(entry.getKey()));
                arguments.add(val);
                returnTypeBuilder.setInnerType(entry.getKey(), val.valueType());
            }

            // Create the function directly to propagate the complete object return type information including
            // inner types. Inner column names would be gone otherwise as the function resolving works on types only.
            return new Function(
                MapFunction.SIGNATURE,
                arguments,
                returnTypeBuilder.build()
            );
        }

        private static final Map<IntervalLiteral.IntervalField, IntervalParser.Precision> INTERVAL_FIELDS =
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

            Comparison gte = new Comparison(coordinatorTxnCtx, nodeCtx, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, value, min);
            Comparison normalizedGte = gte.normalize(context);
            Symbol gteFunc = allocateFunction(
                normalizedGte.operatorName,
                normalizedGte.arguments(),
                context
            );
            Comparison lte = new Comparison(coordinatorTxnCtx, nodeCtx, ComparisonExpression.Type.LESS_THAN_OR_EQUAL, value, max);
            Comparison normalizedLte = lte.normalize(context);
            Symbol lteFunc = allocateFunction(
                normalizedLte.operatorName,
                normalizedLte.arguments(),
                context
            );

            return allocateFunction(AndOperator.NAME, List.of(gteFunc, lteFunc), context);
        }


        @Override
        public Symbol visitMatchPredicate(MatchPredicate node, ExpressionAnalysisContext context) {
            Map<Symbol, Symbol> identBoostMap = HashMap.newHashMap(node.idents().size());
            DataType<?> columnType = null;
            HashSet<RelationName> relationsInColumns = new HashSet<>();
            for (MatchPredicateColumnIdent ident : node.idents()) {
                Symbol column = ident.columnIdent().accept(this, context);
                if (columnType == null) {
                    columnType = column.valueType();
                }
                if (!(column instanceof ScopedSymbol || column instanceof Reference)) {
                    throw new IllegalArgumentException(Symbols.format("can only MATCH on columns, not on %s", column));
                }
                Symbol boost = ident.boost().accept(this, context);
                identBoostMap.put(column, boost);
                if (column instanceof ScopedSymbol scopedSymbol) {
                    relationsInColumns.add(scopedSymbol.relation());
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
            for (Map.Entry<String, Expression> e : node.properties()) {
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
            DataType<?> innerType = fields.getFirst().valueType();
            ArrayType<?> dataType = new ArrayType<>(innerType);
            final SelectSymbol.ResultType resultType;
            if (context.isArrayChild(node)) {
                resultType = SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES;
            } else {
                resultType = SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;
            }
            return new SelectSymbol(relation, dataType, resultType, context.parentIsOrderSensitive());
        }
    }

    private Symbol allocateBuiltinOrUdfFunction(String schema,
                                                String functionName,
                                                List<Symbol> arguments,
                                                Symbol filter,
                                                @Nullable Boolean ignoreNulls,
                                                WindowDefinition windowDefinition,
                                                ExpressionAnalysisContext context) {
        return allocateBuiltinOrUdfFunction(
            schema, functionName, arguments, filter, ignoreNulls, context, windowDefinition, coordinatorTxnCtx, nodeCtx);
    }

    public Symbol allocateFunction(String functionName,
                                    List<Symbol> arguments,
                                    ExpressionAnalysisContext context) {
        return allocateFunction(functionName, arguments, null, context, coordinatorTxnCtx, nodeCtx);
    }

    public static Function allocateFunction(String functionName,
                                            List<Symbol> arguments,
                                            @Nullable Symbol filter,
                                            ExpressionAnalysisContext context,
                                            TransactionContext txnCtx,
                                            NodeContext nodeCtx) {
        return allocateBuiltinOrUdfFunction(
            null, functionName, arguments, filter, null, context, null, txnCtx, nodeCtx);
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
     * @param windowDefinition The definition of the window the allocated function will be executed against.
     * @param txnCtx {@link TransactionContext} for this transaction.
     * @param nodeCtx The {@link NodeContext} to normalize constant expressions.
     * @return The supplied {@link Function} or a {@link Literal} in case of constant folding.
     */
    private static Function allocateBuiltinOrUdfFunction(@Nullable String schema,
                                                         String functionName,
                                                         List<Symbol> arguments,
                                                         @Nullable Symbol filter,
                                                         @Nullable Boolean ignoreNulls,
                                                         ExpressionAnalysisContext context,
                                                         @Nullable WindowDefinition windowDefinition,
                                                         TransactionContext txnCtx,
                                                         NodeContext nodeCtx) {
        FunctionImplementation funcImpl = nodeCtx.functions().get(
            schema,
            functionName,
            arguments,
            txnCtx.sessionSettings().searchPath());

        Signature signature = funcImpl.signature();
        BoundSignature boundSignature = funcImpl.boundSignature();
        List<Symbol> castArguments = cast(arguments, boundSignature.argTypes());
        Function newFunction;
        if (windowDefinition == null) {
            if (signature.getType() == FunctionType.AGGREGATE) {
                context.indicateAggregates();
            } else if (filter != null) {
                throw new UnsupportedOperationException(
                    "Only aggregate functions allow a FILTER clause");
            }
            if (ignoreNulls != null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "%s cannot accept RESPECT or IGNORE NULLS flag.",
                    functionName));
            }
            newFunction = new Function(signature, castArguments, boundSignature.returnType(), filter);
        } else {
            if (signature.getType() != FunctionType.WINDOW) {
                if (signature.getType() != FunctionType.AGGREGATE) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "OVER clause was specified, but %s is neither a window nor an aggregate function.",
                        functionName));
                } else {
                    if (ignoreNulls != null) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "%s cannot accept RESPECT or IGNORE NULLS flag.",
                            functionName));
                    }
                }
            } else if (filter != null) {
                throw new UnsupportedOperationException(
                    "FILTER is not implemented for non-aggregate window functions (" + functionName + ")");
            }
            newFunction = new WindowFunction(
                signature,
                castArguments,
                boundSignature.returnType(),
                filter,
                windowDefinition,
                ignoreNulls);
        }
        return newFunction;
    }

    private static void verifyTypesForMatch(Iterable<? extends Symbol> columns, DataType<?> columnType) {
        if (!io.crate.expression.predicate.MatchPredicate.SUPPORTED_TYPES.contains(columnType)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Can only use MATCH on columns of type STRING or GEO_SHAPE, not on '%s'", columnType));
        }
        for (Symbol column : columns) {
            if (!column.valueType().equals(columnType)) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "All columns within a match predicate must be of the same type. Found %s and %s",
                    columnType, column.valueType()));
            }
        }
    }

    private static void ensureResultTypesMatch(List<? extends Symbol> results) {
        // Structure is [true, default T, condition1 Boolean, value T, condition2 Boolean, value T ...]
        // Skip first pair [true, default T] and then only validate the values
        var resultTypes = HashSet.newHashSet(results.size() / 2);
        for (int i = 3; i < results.size(); i = i + 2) {
            var type = results.get(i).valueType();
            if (type.id() != DataTypes.UNDEFINED.id()) {
                resultTypes.add(type);
            }
        }
        if (resultTypes.size() > 1) {
            var errorMessage = new ArrayList<DataType<?>>(results.size() / 2);
            for (int i = 3; i < results.size(); i = i + 2) {
                errorMessage.add(results.get(i).valueType());
            }
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Data types of all result expressions of a CASE statement must be equal, found: %s",
                                                                  errorMessage));
        }
    }

    @Nullable
    private static SubscriptExpression detectAndGenerateSubscriptExpressions(String columnName) {
        var openSubscriptPos = columnName.indexOf("[");
        if (openSubscriptPos > -1) {
            var sanitizedName = Identifiers.quote(columnName.substring(0, openSubscriptPos)) +
                                columnName.substring(openSubscriptPos);
            try {
                return (SubscriptExpression) SqlParser.createExpression(sanitizedName);
            } catch (ParsingException ignored) {
            }
        }
        return null;
    }

    private static class Comparison {

        private final CoordinatorTxnCtx coordinatorTxnCtx;
        private final NodeContext nodeCtx;
        private ComparisonExpression.Type comparisonExpressionType;
        private Symbol left;
        private Symbol right;
        private String operatorName;

        private Comparison(CoordinatorTxnCtx coordinatorTxnCtx,
                           NodeContext nodeCtx,
                           ComparisonExpression.Type comparisonExpressionType,
                           Symbol left,
                           Symbol right) {
            this.coordinatorTxnCtx = coordinatorTxnCtx;
            this.nodeCtx = nodeCtx;
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
                coordinatorTxnCtx,
                nodeCtx);
            right = null;
            operatorName = NotPredicate.NAME;
        }

        List<Symbol> arguments() {
            if (right == null) {
                // this is the case if the comparison has been rewritten to not(eq(exp1, exp2))
                return List.of(left);
            }
            return List.of(left, right);
        }
    }
}
