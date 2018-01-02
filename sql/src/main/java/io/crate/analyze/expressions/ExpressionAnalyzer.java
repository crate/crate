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
import com.google.common.collect.Multimap;
import io.crate.action.sql.Option;
import io.crate.analyze.DataTypeAnalyzer;
import io.crate.analyze.NegativeLiteralVisitor;
import io.crate.analyze.SubscriptContext;
import io.crate.analyze.SubscriptValidator;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FuncArg;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.operation.aggregation.impl.CollectSetAggregation;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.LikeOperator;
import io.crate.operation.operator.Operator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.operation.operator.RegexpMatchOperator;
import io.crate.operation.operator.any.AnyLikeOperator;
import io.crate.operation.operator.any.AnyNotLikeOperator;
import io.crate.operation.operator.any.AnyOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.scalar.ExtractFunctions;
import io.crate.operation.scalar.SubscriptFunction;
import io.crate.operation.scalar.SubscriptObjectFunction;
import io.crate.operation.scalar.arithmetic.ArrayFunction;
import io.crate.operation.scalar.arithmetic.MapFunction;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.operation.scalar.conditional.IfFunction;
import io.crate.operation.scalar.timestamp.CurrentTimestampFunction;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArithmeticExpression;
import io.crate.sql.tree.ArrayComparison;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BetweenPredicate;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.IfExpression;
import io.crate.sql.tree.InListExpression;
import io.crate.sql.tree.InPredicate;
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
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.SearchedCaseExpression;
import io.crate.sql.tree.SimpleCaseExpression;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;
import io.crate.sql.tree.WhenClause;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SingleColumnTableType;
import io.crate.types.UndefinedType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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

    private static final NegativeLiteralVisitor NEGATIVE_LITERAL_VISITOR = new NegativeLiteralVisitor();
    private final TransactionContext transactionContext;
    private final java.util.function.Function<ParameterExpression, Symbol> convertParamFunction;
    private final FieldProvider<?> fieldProvider;

    @Nullable
    private final SubqueryAnalyzer subQueryAnalyzer;
    private final Functions functions;
    private final InnerExpressionAnalyzer innerAnalyzer;
    private final Operation operation;

    private static final Pattern SUBSCRIPT_SPLIT_PATTERN = Pattern.compile("^([^\\.\\[]+)(\\.*)([^\\[]*)(\\['.*'\\])");

    public ExpressionAnalyzer(Functions functions,
                              TransactionContext transactionContext,
                              java.util.function.Function<ParameterExpression, Symbol> convertParamFunction,
                              FieldProvider fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer) {
        this(functions, transactionContext, convertParamFunction, fieldProvider, subQueryAnalyzer, Operation.READ);
    }

    public ExpressionAnalyzer(Functions functions,
                              TransactionContext transactionContext,
                              java.util.function.Function<ParameterExpression, Symbol> convertParamFunction,
                              FieldProvider fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer,
                              Operation operation) {
        this.functions = functions;
        this.transactionContext = transactionContext;
        this.convertParamFunction = convertParamFunction;
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
        return innerAnalyzer.process(expression, expressionAnalysisContext);
    }

    public Symbol generateQuerySymbol(Optional<Expression> whereExpression, ExpressionAnalysisContext context) {
        if (whereExpression.isPresent()) {
            return convert(whereExpression.get(), context);
        } else {
            return Literal.BOOLEAN_TRUE;
        }
    }

    protected Symbol convertFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
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

        if (node.isDistinct()) {
            if (arguments.size() > 1) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "%s(DISTINCT x) does not accept more than one argument", node.getName()));
            }
            Symbol collectSetFunction = allocateFunction(
                CollectSetAggregation.NAME,
                arguments,
                context);

            // define the outer function which contains the inner function as argument.
            String nodeName = "collection_" + name;
            List<Symbol> outerArguments = ImmutableList.of(collectSetFunction);
            try {
                return allocateBuiltinOrUdfFunction(schema, nodeName, outerArguments, context);
            } catch (UnsupportedOperationException ex) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "unknown function %s(DISTINCT %s)", name, arguments.get(0).valueType()), ex);
            }
        } else {
            return allocateBuiltinOrUdfFunction(schema, name, arguments, context);
        }
    }

    public ExpressionAnalyzer copyForOperation(Operation operation) {
        return new ExpressionAnalyzer(
            functions,
            transactionContext,
            convertParamFunction,
            fieldProvider,
            subQueryAnalyzer,
            operation
        );
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
            castList.add(cast(symbolsToCast.get(i), targetTypes.get(i), false));
        }
        return castList;
    }

    /**
     * Explicitly cast a Symbol to a given target type.
     * @param sourceSymbol The Symbol to cast.
     * @param targetType The type to cast to.
     * @return The Symbol wrapped into a cast function for the given {@code targetType}.
     */
    public static Symbol cast(Symbol sourceSymbol, DataType targetType) {
        return cast(sourceSymbol, targetType, false);
    }

    /**
     * Explicitly cast a Symbol to a given target type.
     * @param sourceSymbol The Symbol to cast.
     * @param targetType The type to cast to.
     * @param tryCast True if a try-cast should be attempted. Try casts return null if the cast fails.
     * @return The Symbol wrapped into a cast function for the given {@code targetType}.
     */
    private static Symbol cast(Symbol sourceSymbol, DataType targetType, boolean tryCast) {
        if (sourceSymbol.valueType().equals(targetType)) {
            return sourceSymbol;
        }
        return sourceSymbol.cast(targetType, tryCast);
    }

    @Nullable
    protected static String getQuotedSubscriptLiteral(String nodeName) {
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
            } else if (!group2.isEmpty() && group3.isEmpty()) {
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
            List<Symbol> args = Lists.newArrayList(
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
            return convertCaseExpressionToIfFunctions(node.getWhenClauses(), node.getOperand(),
                node.getDefaultValue(), context);
        }

        @Override
        protected Symbol visitSearchedCaseExpression(SearchedCaseExpression node, ExpressionAnalysisContext context) {
            return convertCaseExpressionToIfFunctions(node.getWhenClauses(), null, node.getDefaultValue(), context);
        }

        private Symbol convertCaseExpressionToIfFunctions(List<WhenClause> whenClauseExpressions,
                                                          @Nullable Expression operandExpression,
                                                          @Nullable Expression defaultValue,
                                                          ExpressionAnalysisContext context) {
            List<Symbol> operands = new ArrayList<>(whenClauseExpressions.size());
            List<Symbol> results = new ArrayList<>(whenClauseExpressions.size());
            Set<DataType> resultsTypes = new HashSet<>(whenClauseExpressions.size());

            // check for global operand
            Symbol operandLeftSymbol = null;
            if (operandExpression != null) {
                operandLeftSymbol = operandExpression.accept(innerAnalyzer, context);
            }

            for (WhenClause whenClause : whenClauseExpressions) {
                Symbol operandSymbol = whenClause.getOperand().accept(innerAnalyzer, context);

                if (operandLeftSymbol != null) {
                    operandSymbol = allocateFunction(
                        EqOperator.NAME,
                        ImmutableList.of(operandLeftSymbol, operandSymbol),
                        context);
                }

                operands.add(operandSymbol);

                Symbol resultSymbol = whenClause.getResult().accept(innerAnalyzer, context);
                results.add(resultSymbol);
                resultsTypes.add(resultSymbol.valueType());
            }

            if (resultsTypes.size() == 2 && !resultsTypes.contains(DataTypes.UNDEFINED)
                || resultsTypes.size() > 2) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Data types of all result expressions of a CASE statement must be equal, found: %s",
                    resultsTypes));
            }

            Symbol defaultValueSymbol = null;
            if (defaultValue != null) {
                defaultValueSymbol = defaultValue.accept(innerAnalyzer, context);
            }


            return createChain(operands, results, defaultValueSymbol, context);
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
            DataType returnType = DataTypeAnalyzer.convert(node.getType());
            return cast(process(node.getExpression(), context), returnType, false);
        }

        @Override
        protected Symbol visitTryCast(TryCast node, ExpressionAnalysisContext context) {
            DataType returnType = DataTypeAnalyzer.convert(node.getType());

            if (CastFunctionResolver.supportsExplicitConversion(returnType)) {
                try {
                    return cast(process(node.getExpression(), context), returnType, true);
                } catch (ConversionException e) {
                    return Literal.NULL;
                }
            }
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "No cast function found for return type %s", returnType.getName()));
        }

        @Override
        protected Symbol visitExtract(Extract node, ExpressionAnalysisContext context) {
            Symbol expression = process(node.getExpression(), context);
            Symbol field = process(node.getField(), context);
            return allocateFunction(
                ExtractFunctions.GENERIC_NAME,
                ImmutableList.of(field, expression),
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
            return process(arrayComparisonExpression, context);
        }

        @Override
        protected Symbol visitIsNotNullPredicate(IsNotNullPredicate node, ExpressionAnalysisContext context) {
            Symbol argument = process(node.getValue(), context);
            return allocateFunction(
                NotPredicate.NAME,
                ImmutableList.of(
                    allocateFunction(
                        io.crate.operation.predicate.IsNullPredicate.NAME,
                        ImmutableList.of(argument),
                        context)),
                context);
        }

        @Override
        protected Symbol visitSubscriptExpression(SubscriptExpression node, ExpressionAnalysisContext context) {
            SubscriptContext subscriptContext = new SubscriptContext();
            SubscriptValidator.validate(node, subscriptContext);
            return resolveSubscriptSymbol(subscriptContext, context);
        }

        Symbol resolveSubscriptSymbol(SubscriptContext subscriptContext, ExpressionAnalysisContext context) {
            // TODO: support nested subscripts as soon as DataTypes.OBJECT elements can be typed
            Symbol subscriptSymbol;
            Expression subscriptExpression = subscriptContext.expression();
            if (subscriptContext.qualifiedName() != null && subscriptExpression == null) {
                subscriptSymbol = fieldProvider.resolveField(subscriptContext.qualifiedName(), subscriptContext.parts(), operation);
            } else if (subscriptExpression != null) {
                subscriptSymbol = subscriptExpression.accept(this, context);
            } else {
                throw new UnsupportedOperationException("Only references, function calls or array literals " +
                                                        "are valid subscript symbols");
            }
            assert subscriptSymbol != null : "subscriptSymbol must not be null";
            Expression index = subscriptContext.index();
            List<String> parts = subscriptContext.parts();
            if (index != null) {
                Symbol indexSymbol = index.accept(this, context);
                // rewrite array access to subscript scalar
                List<Symbol> args = ImmutableList.of(subscriptSymbol, indexSymbol);
                return allocateFunction(SubscriptFunction.NAME, args, context);
            } else if (parts != null && subscriptExpression != null) {
                Symbol function = allocateFunction(SubscriptObjectFunction.NAME, ImmutableList.of(subscriptSymbol, Literal.of(parts.get(0))), context);
                for (int i = 1; i < parts.size(); i++) {
                    function = allocateFunction(SubscriptObjectFunction.NAME, ImmutableList.of(function, Literal.of(parts.get(i))), context);
                }
                return function;
            }
            return subscriptSymbol;
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
                process(node.getLeft(), context),
                process(node.getRight(), context)
            );
            return allocateFunction(name, arguments, context);
        }

        @Override
        protected Symbol visitNotExpression(NotExpression node, ExpressionAnalysisContext context) {
            Symbol argument = process(node.getValue(), context);
            return allocateFunction(
                NotPredicate.NAME,
                ImmutableList.of(argument),
                context);
        }

        @Override
        protected Symbol visitComparisonExpression(ComparisonExpression node, ExpressionAnalysisContext context) {
            Symbol left = process(node.getLeft(), context);
            Symbol right = process(node.getRight(), context);

            Comparison comparison = new Comparison(functions, transactionContext, node.getType(), left, right);
            comparison.normalize(context);
            FunctionIdent ident = comparison.toFunctionIdent();
            return allocateFunction(ident.name(), comparison.arguments(), context);
        }

        @Override
        public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, ExpressionAnalysisContext context) {
            if (node.quantifier().equals(ArrayComparisonExpression.Quantifier.ALL)) {
                throw new UnsupportedFeatureException("ALL is not supported");
            }

            context.registerArrayComparisonChild(node.getRight());

            Symbol leftSymbol = process(node.getLeft(), context);
            Symbol arraySymbol = process(node.getRight(), context);

            ComparisonExpression.Type operationType = node.getType();
            String operatorName = AnyOperator.OPERATOR_PREFIX + operationType.getValue();
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
            Symbol arraySymbol = process(node.getValue(), context);
            Symbol leftSymbol = process(node.getPattern(), context);

            String operatorName = node.inverse() ? AnyNotLikeOperator.NAME : AnyLikeOperator.NAME;

            ImmutableList<Symbol> arguments = ImmutableList.of(leftSymbol, arraySymbol);

            return allocateFunction(
                operatorName,
                arguments,
                context);
        }

        @Override
        protected Symbol visitLikePredicate(LikePredicate node, ExpressionAnalysisContext context) {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("ESCAPE is not supported.");
            }
            Symbol expression = process(node.getValue(), context);
            Symbol pattern = process(node.getPattern(), context);
            return allocateFunction(
                LikeOperator.NAME,
                ImmutableList.of(expression, pattern),
                context);
        }

        @Override
        protected Symbol visitIsNullPredicate(IsNullPredicate node, ExpressionAnalysisContext context) {
            Symbol value = process(node.getValue(), context);

            return allocateFunction(io.crate.operation.predicate.IsNullPredicate.NAME, ImmutableList.of(value), context);
        }

        @Override
        protected Symbol visitNegativeExpression(NegativeExpression node, ExpressionAnalysisContext context) {
            // in statements like "where x = -1" the  positive (expression)IntegerLiteral (1)
            // is just wrapped inside a negativeExpression
            // the visitor here swaps it to getBuiltin -1 in a (symbol)LiteralInteger
            return NEGATIVE_LITERAL_VISITOR.process(process(node.getValue(), context), null);
        }

        @Override
        protected Symbol visitArithmeticExpression(ArithmeticExpression node, ExpressionAnalysisContext context) {
            Symbol left = process(node.getLeft(), context);
            Symbol right = process(node.getRight(), context);

            return allocateFunction(
                node.getType().name().toLowerCase(Locale.ENGLISH),
                ImmutableList.of(left, right),
                context);
        }

        @Override
        protected Symbol visitQualifiedNameReference(QualifiedNameReference node, ExpressionAnalysisContext context) {
            try {
                return fieldProvider.resolveField(node.getName(), null, operation);
            } catch (ColumnUnknownException exception) {
                if (transactionContext.sessionContext().options().contains(Option.ALLOW_QUOTED_SUBSCRIPT)) {
                    String quotedSubscriptLiteral = getQuotedSubscriptLiteral(node.getName().toString());
                    if (quotedSubscriptLiteral != null) {
                        return process(SqlParser.createExpression(quotedSubscriptLiteral), context);
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
                return Literal.of(new ArrayType(UndefinedType.INSTANCE), new Object[0]);
            } else {
                List<Symbol> arguments = new ArrayList<>(values.size());
                for (Expression value : values) {
                    arguments.add(process(value, context));
                }
                return allocateFunction(
                    ArrayFunction.NAME,
                    arguments,
                    context);
            }
        }

        @Override
        public Symbol visitObjectLiteral(ObjectLiteral node, ExpressionAnalysisContext context) {
            Multimap<String, Expression> values = node.values();
            if (values.isEmpty()) {
                return Literal.EMPTY_OBJECT;
            }
            List<Symbol> arguments = new ArrayList<>(values.size() * 2);
            for (Map.Entry<String, Expression> entry : values.entries()) {
                arguments.add(Literal.of(entry.getKey()));
                arguments.add(process(entry.getValue(), context));
            }
            return allocateFunction(
                MapFunction.NAME,
                arguments,
                context);
        }

        @Override
        public Symbol visitParameterExpression(ParameterExpression node, ExpressionAnalysisContext context) {
            return convertParamFunction.apply(node);
        }

        @Override
        protected Symbol visitBetweenPredicate(BetweenPredicate node, ExpressionAnalysisContext context) {
            // <value> between <min> and <max>
            // -> <value> >= <min> and <value> <= max
            Symbol value = process(node.getValue(), context);
            Symbol min = process(node.getMin(), context);
            Symbol max = process(node.getMax(), context);

            Comparison gte = new Comparison(functions, transactionContext, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, value, min);
            FunctionIdent gteIdent = gte.normalize(context).toFunctionIdent();
            Symbol gteFunc = allocateFunction(
                gteIdent.name(),
                gte.arguments(),
                context);

            Comparison lte = new Comparison(functions, transactionContext, ComparisonExpression.Type.LESS_THAN_OR_EQUAL, value, max);
            FunctionIdent lteIdent = lte.normalize(context).toFunctionIdent();
            Symbol lteFunc = allocateFunction(
                lteIdent.name(),
                lte.arguments(),
                context);

            return allocateFunction(AndOperator.NAME, ImmutableList.of(gteFunc, lteFunc), context);
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate node, ExpressionAnalysisContext context) {
            Map<Field, Symbol> identBoostMap = new HashMap<>(node.idents().size());
            DataType columnType = null;
            for (MatchPredicateColumnIdent ident : node.idents()) {
                Symbol column = process(ident.columnIdent(), context);
                if (columnType == null) {
                    columnType = column.valueType();
                }
                Preconditions.checkArgument(
                    column instanceof Field,
                    SymbolFormatter.format("can only MATCH on columns, not on %s", column));
                Symbol boost = process(ident.boost(), context);
                identBoostMap.put(((Field) column), boost);
            }
            assert columnType != null : "columnType must not be null";
            verifyTypesForMatch(identBoostMap.keySet(), columnType);

            Symbol queryTerm = cast(process(node.value(), context), columnType);
            String matchType = io.crate.operation.predicate.MatchPredicate.getMatchType(node.matchType(), columnType);

            List<Symbol> mapArgs = new ArrayList<>(node.properties().size() * 2);
            for (Map.Entry<String, Expression> e : node.properties().properties().entrySet()) {
                mapArgs.add(Literal.of(e.getKey()));
                mapArgs.add(process(e.getValue(), context));
            }
            Symbol options = allocateFunction(MapFunction.NAME, mapArgs, context);
            return new io.crate.analyze.symbol.MatchPredicate(identBoostMap, queryTerm, matchType, options);
        }

        @Override
        protected Symbol visitSubqueryExpression(SubqueryExpression node, ExpressionAnalysisContext context) {
            if (subQueryAnalyzer == null) {
                throw new UnsupportedOperationException("Subquery not supported in this statement");
            }
            /* note: This does not support analysis columns in the subquery which belong to the parent relation
             * this would require {@link StatementAnalysisContext#startRelation} to somehow inherit the parent context
             */
            QueriedRelation relation = subQueryAnalyzer.analyze(node.getQuery());
            List<Field> fields = relation.fields();
            if (fields.size() > 1) {
                throw new UnsupportedOperationException("Subqueries with more than 1 column are not supported.");
            }
            /*
             * The SelectSymbol should actually have a RowType as it is a row-expression.
             *
             * But there are no other row-expressions yet. In addition the cast functions and operators don't work with
             * row types (yet).
             *
             * However, we support a single column RowType through the SingleColumnTableType.
             */
            DataType innerType = fields.get(0).valueType();
            SingleColumnTableType dataType = new SingleColumnTableType(innerType);
            final SelectSymbol.ResultType resultType;
            if (context.isArrayComparisonChild(node)) {
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
                                                ExpressionAnalysisContext context) {
        return allocateBuiltinOrUdfFunction(schema, functionName, arguments, context, functions, transactionContext);
    }

    private Symbol allocateFunction(String functionName,
                                    List<Symbol> arguments,
                                    ExpressionAnalysisContext context) {
        return allocateFunction(functionName, arguments, context, functions, transactionContext);
    }

    static Symbol allocateFunction(String functionName,
                                   List<Symbol> arguments,
                                   ExpressionAnalysisContext context,
                                   Functions functions,
                                   TransactionContext transactionContext) {
        return allocateBuiltinOrUdfFunction(null, functionName, arguments, context, functions, transactionContext);
    }

    /**
     * Creates a function symbol and tries to normalize the new function's
     * {@link FunctionImplementation} iff only Literals are supplied as arguments.
     * This folds any constant expressions like '1 + 1' => '2'.
     * @param schema The schema for udf functions
     * @param functionName The function name of the new function.
     * @param arguments The arguments to provide to the {@link Function}.
     * @param context Context holding the state for the current translation.
     * @param functions The {@link Functions} to normalize constant expressions.
     * @param transactionContext {@link TransactionContext} for this transaction.
     * @return The supplied {@link Function} or a {@link Literal} in case of constant folding.
     */
    private static Symbol allocateBuiltinOrUdfFunction(@Nullable String schema,
                                                       String functionName,
                                                       List<Symbol> arguments,
                                                       ExpressionAnalysisContext context,
                                                       Functions functions,
                                                       TransactionContext transactionContext) {
        FunctionImplementation funcImpl = getFuncImpl(
            schema,
            transactionContext.sessionContext().defaultSchema(),
            functionName,
            arguments,
            functions);

        FunctionInfo functionInfo = funcImpl.info();
        if (functionInfo.type() == FunctionInfo.Type.AGGREGATE) {
            context.indicateAggregates();
        }
        List<Symbol> castArguments = cast(arguments, functionInfo.ident().argumentTypes());
        Function newFunction = new Function(functionInfo, castArguments);
        if (functionInfo.isDeterministic() && Symbols.allLiterals(newFunction)) {
            return funcImpl.normalizeSymbol(newFunction, transactionContext);
        }
        return newFunction;
    }


    private static FunctionImplementation getFuncImpl(@Nullable String schemaProvided,
                                                      String defaultSchema,
                                                      String functionName,
                                                      List<? extends FuncArg> arguments,
                                                      Functions functions) {
        FunctionImplementation funcImpl;
        if (schemaProvided == null) {
            funcImpl = functions.getBuiltinByArgs(functionName, arguments);
            if (funcImpl == null) {
                funcImpl = functions.getUserDefinedByArgs(defaultSchema, functionName, arguments);
            }
        } else {
            return functions.getUserDefinedByArgs(schemaProvided, functionName, arguments);
        }
        if (funcImpl == null) {
            throw Functions.createUnknownFunctionExceptionFromArgs(functionName, arguments);
        }
        return funcImpl;
    }

    private static void verifyTypesForMatch(Iterable<? extends Symbol> columns, DataType columnType) {
        Preconditions.checkArgument(
            io.crate.operation.predicate.MatchPredicate.SUPPORTED_TYPES.contains(columnType),
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

    private static class Comparison {

        private final Functions functions;
        private final TransactionContext transactionContext;
        private ComparisonExpression.Type comparisonExpressionType;
        private Symbol left;
        private Symbol right;
        private String operatorName;
        private FunctionIdent functionIdent;

        private Comparison(Functions functions,
                           TransactionContext transactionContext,
                           ComparisonExpression.Type comparisonExpressionType,
                           Symbol left,
                           Symbol right) {
            this.functions = functions;
            this.transactionContext = transactionContext;
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
            if ((!(right instanceof Reference || right instanceof Field)
                || left instanceof Reference || left instanceof Field)
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
                ImmutableList.of(left, right),
                context,
                functions,
                transactionContext);
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
