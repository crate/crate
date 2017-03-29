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
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.DataTypeAnalyzer;
import io.crate.analyze.NegativeLiteralVisitor;
import io.crate.analyze.SubscriptContext;
import io.crate.analyze.SubscriptValidator;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.table.Operation;
import io.crate.operation.aggregation.impl.CollectSetAggregation;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.AnyEqOperator;
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
import io.crate.sql.tree.*;
import io.crate.sql.tree.MatchPredicate;
import io.crate.types.*;

import javax.annotation.Nullable;
import java.util.*;
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

    private final static Map<ComparisonExpression.Type, ComparisonExpression.Type> SWAP_OPERATOR_TABLE =
        ImmutableMap.<ComparisonExpression.Type, ComparisonExpression.Type>builder()
            .put(ComparisonExpression.Type.GREATER_THAN, ComparisonExpression.Type.LESS_THAN)
            .put(ComparisonExpression.Type.LESS_THAN, ComparisonExpression.Type.GREATER_THAN)
            .put(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ComparisonExpression.Type.LESS_THAN_OR_EQUAL)
            .put(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL)
            .build();

    private final static DataTypeAnalyzer DATA_TYPE_ANALYZER = new DataTypeAnalyzer();
    private final static NegativeLiteralVisitor NEGATIVE_LITERAL_VISITOR = new NegativeLiteralVisitor();
    private final SessionContext sessionContext;
    private final java.util.function.Function<ParameterExpression, Symbol> convertParamFunction;
    private final FieldProvider<?> fieldProvider;

    @Nullable
    private final SubqueryAnalyzer subQueryAnalyzer;
    private final Functions functions;
    private final InnerExpressionAnalyzer innerAnalyzer;
    private Operation operation = Operation.READ;

    private static final Pattern SUBSCRIPT_SPLIT_PATTERN = Pattern.compile("^([^\\.\\[]+)(\\.*)([^\\[]*)(\\['.*'\\])");

    public ExpressionAnalyzer(Functions functions,
                              SessionContext sessionContext,
                              java.util.function.Function<ParameterExpression, Symbol> convertParamFunction,
                              FieldProvider fieldProvider,
                              @Nullable SubqueryAnalyzer subQueryAnalyzer) {
        this.functions = functions;
        this.sessionContext = sessionContext;
        this.convertParamFunction = convertParamFunction;
        this.fieldProvider = fieldProvider;
        this.subQueryAnalyzer = subQueryAnalyzer;
        this.innerAnalyzer = new InnerExpressionAnalyzer();
    }

    /**
     * <h2>Converts an expression into a symbol.</h2>
     * <p>
     * <p>
     * Expressions like QualifiedName that reference a column are resolved using the fieldResolver that were passed
     * to the constructor.
     * </p>
     * <p>
     * <p>
     * Some information (like resolved function symbols) are written onto the given expressionAnalysisContext
     * </p>
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

    private FunctionInfo getFunctionInfo(FunctionIdent ident) {
        return functions.getSafe(ident).info();
    }

    protected Symbol convertFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
        List<Symbol> arguments = new ArrayList<>(node.getArguments().size());
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            Symbol argSymbol = expression.accept(innerAnalyzer, context);

            argumentTypes.add(argSymbol.valueType());
            arguments.add(argSymbol);
        }

        FunctionInfo functionInfo;
        if (node.isDistinct()) {
            if (argumentTypes.size() > 1) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "%s(DISTINCT x) does not accept more than one argument", node.getName()));
            }
            // define the inner function. use the arguments/argumentTypes from above
            FunctionIdent innerIdent = new FunctionIdent(CollectSetAggregation.NAME, argumentTypes);
            FunctionInfo innerInfo = getFunctionInfo(innerIdent);
            Symbol innerFunction = context.allocateFunction(innerInfo, arguments);

            // define the outer function which contains the inner function as argument.
            String nodeName = "collection_" + node.getName().toString();
            List<Symbol> outerArguments = Arrays.<Symbol>asList(innerFunction);
            ImmutableList<DataType> outerArgumentTypes =
                ImmutableList.<DataType>of(new SetType(argumentTypes.get(0)));

            FunctionIdent ident = new FunctionIdent(nodeName, outerArgumentTypes);
            try {
                functionInfo = getFunctionInfo(ident);
            } catch (UnsupportedOperationException ex) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "unknown function %s(DISTINCT %s)", node.getName(), argumentTypes.get(0)), ex);
            }
            arguments = outerArguments;
        } else {
            FunctionIdent ident = new FunctionIdent(node.getName().toString(), argumentTypes);
            functionInfo = getFunctionInfo(ident);
        }
        return context.allocateFunction(functionInfo, arguments);
    }

    public void setResolveFieldsOperation(Operation operation) {
        this.operation = operation;
    }

    public static Symbol castIfNeededOrFail(Symbol symbolToCast, DataType targetType) {
        DataType sourceType = symbolToCast.valueType();
        if (sourceType.equals(targetType)) {
            return symbolToCast;
        }
        if (sourceType.isConvertableTo(targetType)) {
            // cast further below doesn't always fail because it might be lazy as it wraps functions/references inside a cast function.
            // -> Need to check isConvertableTo to fail eagerly if the cast won't work.
            return cast(symbolToCast, targetType, false);
        }
        throw new ConversionException(symbolToCast, targetType);
    }

    private static Symbol cast(Symbol sourceSymbol, DataType targetType, boolean tryCast) {
        if (sourceSymbol.symbolType().isValueSymbol()) {
            return Literal.convert(sourceSymbol, targetType);
        }
        return CastFunctionResolver.generateCastFunction(sourceSymbol, targetType, tryCast);
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
                "Unsupported expression %s", ExpressionFormatter.formatExpression(node)));
        }

        @Override
        protected Symbol visitCurrentTime(CurrentTime node, ExpressionAnalysisContext context) {
            if (!node.getType().equals(CurrentTime.Type.TIMESTAMP)) {
                visitExpression(node, context);
            }
            List<Symbol> args = Lists.<Symbol>newArrayList(
                Literal.of(node.getPrecision().or(CurrentTimestampFunction.DEFAULT_PRECISION))
            );
            return context.allocateFunction(CurrentTimestampFunction.INFO, args);
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
            return IfFunction.createFunction(arguments);
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
                Symbol operandRightSymbol = whenClause.getOperand().accept(innerAnalyzer, context);
                Symbol operandSymbol = operandRightSymbol;

                if (operandLeftSymbol != null) {
                    operandSymbol = EqOperator.createFunction(
                        operandLeftSymbol, castIfNeededOrFail(operandRightSymbol, operandLeftSymbol.valueType()));
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

            return IfFunction.createChain(operands, results, defaultValueSymbol);
        }

        @Override
        protected Symbol visitCast(Cast node, ExpressionAnalysisContext context) {
            DataType returnType = DATA_TYPE_ANALYZER.process(node.getType(), null);
            return cast(process(node.getExpression(), context), returnType, false);
        }

        @Override
        protected Symbol visitTryCast(TryCast node, ExpressionAnalysisContext context) {
            DataType returnType = DATA_TYPE_ANALYZER.process(node.getType(), null);

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
            expression = castIfNeededOrFail(expression, DataTypes.TIMESTAMP);
            Symbol field = castIfNeededOrFail(process(node.getField(), context), DataTypes.STRING);
            return context.allocateFunction(
                ExtractFunctions.GENERIC_INFO, Arrays.asList(field, expression));
        }

        @Override
        protected Symbol visitInPredicate(InPredicate node, ExpressionAnalysisContext context) {
            /*
             * convert where x IN (values)
             *
             * where values = a list of expressions
             *
             * into
             *
             *      x = ANY(array(1, 2, 3, ...))
             */
            Symbol left = process(node.getValue(), context);
            DataType targetType = left.valueType();

            Expression valueList = node.getValueList();
            if (!(valueList instanceof InListExpression)) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Expression %s is not supported in IN", ExpressionFormatter.formatExpression(valueList)));
            }
            List<Expression> expressions = ((InListExpression) valueList).getValues();
            List<Symbol> symbols = new ArrayList<>(expressions.size());

            for (Expression expression : expressions) {
                Symbol symbol = process(expression, context);
                if (targetType == DataTypes.UNDEFINED) {
                    targetType = symbol.valueType();
                    left = castIfNeededOrFail(left, targetType);

                    symbols.add(symbol);
                } else {
                    symbols.add(castIfNeededOrFail(symbol, targetType));
                }
            }
            return context.allocateFunction(
                AnyEqOperator.createInfo(targetType),
                Arrays.asList(
                    left, context.allocateFunction(ArrayFunction.createInfo(Symbols.extractTypes(symbols)), symbols))
            );
        }

        @Override
        protected Symbol visitIsNotNullPredicate(IsNotNullPredicate node, ExpressionAnalysisContext context) {
            Symbol argument = process(node.getValue(), context);

            FunctionIdent isNullIdent =
                new FunctionIdent(io.crate.operation.predicate.IsNullPredicate.NAME, ImmutableList.of(argument.valueType()));
            FunctionInfo isNullInfo = getFunctionInfo(isNullIdent);

            return context.allocateFunction(
                NotPredicate.INFO,
                Arrays.<Symbol>asList(context.allocateFunction(isNullInfo, Arrays.asList(argument))));
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
            if (subscriptContext.qName() != null && subscriptExpression == null) {
                subscriptSymbol = fieldProvider.resolveField(subscriptContext.qName(), subscriptContext.parts(), operation);
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
                FunctionIdent functionIdent = new FunctionIdent(SubscriptFunction.NAME,
                    ImmutableList.of(subscriptSymbol.valueType(), indexSymbol.valueType()));
                return context.allocateFunction(getFunctionInfo(functionIdent),
                    Arrays.asList(subscriptSymbol, indexSymbol));
            } else if (parts != null && subscriptExpression != null) {
                FunctionIdent ident = new FunctionIdent(
                    SubscriptObjectFunction.NAME,
                    ImmutableList.of(subscriptSymbol.valueType(), DataTypes.STRING)
                );
                FunctionInfo info = getFunctionInfo(ident);

                Symbol function = context.allocateFunction(info, Arrays.asList(subscriptSymbol, Literal.of(parts.get(0))));
                for (int i = 1; i < parts.size(); i++) {
                    function = context.allocateFunction(info, Arrays.asList(function, Literal.of(parts.get(i))));
                }
                return function;
            }
            return subscriptSymbol;
        }

        @Override
        protected Symbol visitLogicalBinaryExpression(LogicalBinaryExpression node, ExpressionAnalysisContext context) {
            FunctionInfo functionInfo;
            switch (node.getType()) {
                case AND:
                    functionInfo = AndOperator.INFO;
                    break;
                case OR:
                    functionInfo = OrOperator.INFO;
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported logical binary expression " + node.getType().name());
            }
            List<Symbol> arguments = new ArrayList<>(2);
            arguments.add(process(node.getLeft(), context));
            arguments.add(process(node.getRight(), context));
            return context.allocateFunction(functionInfo, arguments);
        }

        @Override
        protected Symbol visitNotExpression(NotExpression node, ExpressionAnalysisContext context) {
            Symbol argument = process(node.getValue(), context);

            FunctionIdent functionIdent = new FunctionIdent(NotPredicate.NAME, Arrays.asList(argument.valueType()));
            return context.allocateFunction(getFunctionInfo(functionIdent), Arrays.asList(argument));
        }

        @Override
        protected Symbol visitComparisonExpression(ComparisonExpression node, ExpressionAnalysisContext context) {
            Symbol left = process(node.getLeft(), context);
            Symbol right = process(node.getRight(), context);

            Comparison comparison = new Comparison(node.getType(), left, right);
            comparison.normalize(context);
            FunctionInfo info = getFunctionInfo(comparison.toFunctionIdent());
            return context.allocateFunction(info, comparison.arguments());
        }

        @Override
        public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, ExpressionAnalysisContext context) {
            if (node.quantifier().equals(ArrayComparisonExpression.Quantifier.ALL)) {
                throw new UnsupportedFeatureException("ALL is not supported");
            }

            Symbol arraySymbol = process(node.getRight(), context);
            Symbol leftSymbol = process(node.getLeft(), context);
            DataType rightType = arraySymbol.valueType();

            if (!DataTypes.isCollectionType(rightType)) {
                throw new IllegalArgumentException(
                    SymbolFormatter.format("invalid array expression: '%s'", arraySymbol));
            }
            DataType rightInnerType = ((CollectionType) rightType).innerType();
            if (rightInnerType.equals(DataTypes.OBJECT)) {
                throw new IllegalArgumentException("ANY on object arrays is not supported");
            }

            // There is no proper type-precedence logic in place yet,
            // but in this case the side which has a column instead of functions/literals should dictate the type.

            // x = ANY([null])              -> must not result in to-null casts
            // int_col = ANY([1, 2, 3])     -> must cast to int instead of long (otherwise lucene query would be slow)
            // null = ANY([1, 2])           -> must not cast to null
            if (SymbolVisitors.any(Predicates.instanceOf(Field.class), leftSymbol) || rightInnerType == DataTypes.UNDEFINED) {
                arraySymbol = castIfNeededOrFail(arraySymbol, new ArrayType(leftSymbol.valueType()));
            } else {
                leftSymbol = castIfNeededOrFail(leftSymbol, rightInnerType);
            }

            ComparisonExpression.Type operationType = node.getType();
            String operatorName;
            operatorName = AnyOperator.OPERATOR_PREFIX + operationType.getValue();
            FunctionIdent functionIdent = new FunctionIdent(operatorName, Arrays.asList(leftSymbol.valueType(), arraySymbol.valueType()));
            FunctionInfo functionInfo = getFunctionInfo(functionIdent);
            return context.allocateFunction(functionInfo, Arrays.asList(leftSymbol, arraySymbol));
        }

        @Override
        public Symbol visitArrayLikePredicate(ArrayLikePredicate node, ExpressionAnalysisContext context) {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("ESCAPE is not supported.");
            }
            Symbol rightSymbol = process(node.getValue(), context);
            Symbol leftSymbol = process(node.getPattern(), context);
            DataType rightType = rightSymbol.valueType();

            if (!DataTypes.isCollectionType(rightType)) {
                throw new IllegalArgumentException(
                    SymbolFormatter.format("invalid array expression: '%s'", rightSymbol));
            }
            rightSymbol = castIfNeededOrFail(rightSymbol, new ArrayType(DataTypes.STRING));
            String operatorName = node.inverse() ? AnyNotLikeOperator.NAME : AnyLikeOperator.NAME;

            FunctionIdent functionIdent = new FunctionIdent(operatorName, Arrays.asList(leftSymbol.valueType(), rightSymbol.valueType()));
            FunctionInfo functionInfo = getFunctionInfo(functionIdent);
            return context.allocateFunction(functionInfo, Arrays.asList(leftSymbol, rightSymbol));
        }

        @Override
        protected Symbol visitLikePredicate(LikePredicate node, ExpressionAnalysisContext context) {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("ESCAPE is not supported.");
            }
            Symbol expression = process(node.getValue(), context);
            expression = castIfNeededOrFail(expression, DataTypes.STRING);
            Symbol pattern = castIfNeededOrFail(process(node.getPattern(), context), DataTypes.STRING);
            FunctionIdent functionIdent = FunctionIdent.of(LikeOperator.NAME, expression.valueType(), pattern.valueType());
            return context.allocateFunction(getFunctionInfo(functionIdent), Arrays.asList(expression, pattern));
        }

        @Override
        protected Symbol visitIsNullPredicate(IsNullPredicate node, ExpressionAnalysisContext context) {
            Symbol value = process(node.getValue(), context);

            FunctionIdent functionIdent =
                new FunctionIdent(io.crate.operation.predicate.IsNullPredicate.NAME, ImmutableList.of(value.valueType()));
            FunctionInfo functionInfo = getFunctionInfo(functionIdent);
            return context.allocateFunction(functionInfo, Arrays.asList(value));
        }

        @Override
        protected Symbol visitNegativeExpression(NegativeExpression node, ExpressionAnalysisContext context) {
            // in statements like "where x = -1" the  positive (expression)IntegerLiteral (1)
            // is just wrapped inside a negativeExpression
            // the visitor here swaps it to get -1 in a (symbol)LiteralInteger
            return NEGATIVE_LITERAL_VISITOR.process(process(node.getValue(), context), null);
        }

        @Override
        protected Symbol visitArithmeticExpression(ArithmeticExpression node, ExpressionAnalysisContext context) {
            Symbol left = process(node.getLeft(), context);
            Symbol right = process(node.getRight(), context);

            FunctionIdent functionIdent = new FunctionIdent(
                node.getType().name().toLowerCase(Locale.ENGLISH),
                Arrays.asList(left.valueType(), right.valueType())
            );
            return context.allocateFunction(getFunctionInfo(functionIdent), Arrays.asList(left, right));
        }

        @Override
        protected Symbol visitQualifiedNameReference(QualifiedNameReference node, ExpressionAnalysisContext context) {
            try {
                return fieldProvider.resolveField(node.getName(), null, operation);
            } catch (ColumnUnknownException exception) {
                if (sessionContext.options().contains(Option.ALLOW_QUOTED_SUBSCRIPT)) {
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
                FunctionIdent functionIdent = new FunctionIdent(ArrayFunction.NAME, Symbols.extractTypes(arguments));
                return context.allocateFunction(getFunctionInfo(functionIdent), arguments);
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
            FunctionIdent functionIdent = new FunctionIdent(MapFunction.NAME, Symbols.extractTypes(arguments));
            return context.allocateFunction(getFunctionInfo(functionIdent), arguments);
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

            Comparison gte = new Comparison(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, value, min);
            Function gteFunc = context.allocateFunction(
                getFunctionInfo(gte.normalize(context).toFunctionIdent()), gte.arguments());

            Comparison lte = new Comparison(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, value, max);
            Function lteFunc = context.allocateFunction(
                getFunctionInfo(lte.normalize(context).toFunctionIdent()), lte.arguments());

            return AndOperator.of(gteFunc, lteFunc);
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

            Symbol queryTerm = castIfNeededOrFail(process(node.value(), context), columnType);
            String matchType = io.crate.operation.predicate.MatchPredicate.getMatchType(node.matchType(), columnType);

            List<Symbol> mapArgs = new ArrayList<>(node.properties().size() * 2);
            for (Map.Entry<String, Expression> e : node.properties().properties().entrySet()) {
                mapArgs.add(Literal.of(e.getKey()));
                mapArgs.add(process(e.getValue(), context));
            }
            Function options = context.allocateFunction(MapFunction.createInfo(Symbols.extractTypes(mapArgs)), mapArgs);
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
            AnalyzedRelation relation = subQueryAnalyzer.analyze(node.getQuery());
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
             * Since we only support 1 column and only single-row subselects it is okay to use the inner type directly.
             */
            return new SelectSymbol(relation, fields.get(0).valueType());
        }

        private void verifyTypesForMatch(Iterable<? extends Symbol> columns, DataType columnType) {
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
    }

    private static class Comparison {

        private static final Set<ComparisonExpression.Type> NEGATING_TYPES = ImmutableSet.of(
            ComparisonExpression.Type.REGEX_NO_MATCH,
            ComparisonExpression.Type.REGEX_NO_MATCH_CI,
            ComparisonExpression.Type.NOT_EQUAL);

        private ComparisonExpression.Type comparisonExpressionType;
        private Symbol left;
        private Symbol right;
        private DataType leftType;
        private DataType rightType;
        private String operatorName;
        private FunctionIdent functionIdent = null;

        private Comparison(ComparisonExpression.Type comparisonExpressionType,
                           Symbol left,
                           Symbol right) {
            this.operatorName = "op_" + comparisonExpressionType.getValue();
            this.comparisonExpressionType = comparisonExpressionType;
            this.left = left;
            this.right = right;
            this.leftType = left.valueType();
            this.rightType = right.valueType();
        }

        Comparison normalize(ExpressionAnalysisContext context) {
            swapIfNecessary();
            castTypes();
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
                && leftType.id() != DataTypes.UNDEFINED.id()) {
                return;
            }
            ComparisonExpression.Type type = SWAP_OPERATOR_TABLE.get(comparisonExpressionType);
            if (type != null) {
                comparisonExpressionType = type;
                operatorName = "op_" + type.getValue();
            }
            Symbol tmp = left;
            DataType tmpType = leftType;
            left = right;
            leftType = rightType;
            right = tmp;
            rightType = tmpType;
        }

        private void castTypes() {
            right = castIfNeededOrFail(right, leftType);
            rightType = leftType;
        }

        /**
         * rewrite   exp1 != exp2  to not(eq(exp1, exp2))
         * and       exp1 !~ exp2  to not(~(exp1, exp2))
         * does nothing if operator != not equals
         */
        private void rewriteNegatingOperators(ExpressionAnalysisContext context) {
            if (!NEGATING_TYPES.contains(comparisonExpressionType)) {
                return;
            }

            String opName = null;
            DataType opType = null;
            switch (comparisonExpressionType) {
                case NOT_EQUAL:
                    opName = EqOperator.NAME;
                    opType = EqOperator.RETURN_TYPE;
                    break;
                case REGEX_NO_MATCH:
                    opName = RegexpMatchOperator.NAME;
                    opType = RegexpMatchOperator.RETURN_TYPE;
                    break;
                case REGEX_NO_MATCH_CI:
                    opName = RegexpMatchCaseInsensitiveOperator.NAME;
                    opType = RegexpMatchCaseInsensitiveOperator.RETURN_TYPE;
                    break;
            }

            FunctionIdent ident = new FunctionIdent(opName, Arrays.asList(leftType, rightType));
            left = context.allocateFunction(
                new FunctionInfo(ident, opType),
                Arrays.asList(left, right)
            );
            right = null;
            rightType = null;
            functionIdent = NotPredicate.INFO.ident();
            leftType = opType;
            operatorName = NotPredicate.NAME;
        }

        FunctionIdent toFunctionIdent() {
            if (functionIdent == null) {
                return new FunctionIdent(operatorName, Arrays.asList(leftType, rightType));
            }
            return functionIdent;
        }

        List<Symbol> arguments() {
            if (right == null) {
                // this is the case if the comparison has been rewritten to not(eq(exp1, exp2))
                return Arrays.asList(left);
            }
            return Arrays.asList(left, right);
        }
    }
}
