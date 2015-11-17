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

package io.crate.analyze.expressions;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.action.sql.SQLBaseRequest;
import io.crate.analyze.*;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.Literal;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CollectSetAggregation;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.operation.operator.any.AnyLikeOperator;
import io.crate.operation.operator.any.AnyNotLikeOperator;
import io.crate.operation.operator.any.AnyOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.scalar.ExtractFunctions;
import io.crate.operation.scalar.SubscriptFunction;
import io.crate.operation.scalar.cast.CastFunctionResolver;
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

import static io.crate.analyze.symbol.Literal.newLiteral;

/**
 * <p>This Analyzer can be used to convert Expression from the SQL AST into symbols.</p>
 *
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
    private final static SubscriptVisitor SUBSCRIPT_VISITOR = new SubscriptVisitor();
    private final EvaluatingNormalizer normalizer;
    private final FieldProvider fieldProvider;
    private final Functions functions;
    private final Schemas schemas;
    protected final ParameterContext parameterContext;
    protected InnerExpressionAnalyzer innerAnalyzer;
    private boolean forWrite = false;

    private static final Pattern SUBSCRIPT_SPLIT_PATTERN = Pattern.compile("^([^\\.\\[]+)(\\.*)([^\\[]*)(\\['.*'\\])");

    public ExpressionAnalyzer(AnalysisMetaData analysisMetaData,
                              ParameterContext parameterContext,
                              FieldProvider fieldProvider,
                              @Nullable FieldResolver fieldResolver) {
        functions = analysisMetaData.functions();
        schemas = analysisMetaData.referenceInfos();
        this.parameterContext = parameterContext;
        this.fieldProvider = fieldProvider;
        this.innerAnalyzer = new InnerExpressionAnalyzer();
        this.normalizer = new EvaluatingNormalizer(
                analysisMetaData.functions(), RowGranularity.CLUSTER, analysisMetaData.referenceResolver(), fieldResolver, false);
    }

    @Nullable
    public Integer integerFromExpression(Optional<Expression> expression) {
        if (expression.isPresent()) {
            return DataTypes.INTEGER.value(
                    ExpressionToNumberVisitor.convert(expression.get(), parameterContext.parameters()));
        }
        return null;
    }

    /**
     * Use to normalize a symbol. (For example to normalize a function that contains only literals to a literal)
     */
    public Symbol normalize(Symbol symbol) {
        return normalizer.normalize(symbol);
    }

    /**
     * <h2>Converts a expression into a symbol.</h2>
     *
     * <p>
     * Expressions like QualifiedName that reference a column are resolved using the fieldResolver that were passed
     * to the constructor.
     * </p>
     *
     * <p>
     * Some information (like resolved function symbols) are written onto the given expressionAnalysisContext
     * </p>
     */
    public Symbol convert(Expression expression, ExpressionAnalysisContext expressionAnalysisContext) {
        return innerAnalyzer.process(expression, expressionAnalysisContext);
    }

    public WhereClause generateWhereClause(Optional<Expression> whereExpression, ExpressionAnalysisContext context) {
        if (whereExpression.isPresent()) {
            return new WhereClause(normalize(convert(whereExpression.get(), context)), null, null);
        } else {
            return WhereClause.MATCH_ALL;
        }
    }

    protected FunctionInfo getFunctionInfo(FunctionIdent ident) {
        return functions.getSafe(ident).info();
    }

    /**
     * normalize and validate given value according to the corresponding {@link io.crate.analyze.symbol.Reference}
     *
     * @param valueSymbol the value to normalize, might be anything from {@link io.crate.metadata.Scalar} to {@link io.crate.analyze.symbol.Literal}
     * @param reference   the reference to which the value has to comply in terms of type-compatibility
     * @return the normalized Symbol, should be a literal
     * @throws io.crate.exceptions.ColumnValidationException
     */
    public Symbol normalizeInputForReference(
            Symbol valueSymbol, Reference reference, ExpressionAnalysisContext context) {

        Literal literal = null;
        try {
            valueSymbol = normalizer.normalize(valueSymbol);
            assert valueSymbol != null : "valueSymbol must not be null";
            if (valueSymbol.symbolType() != SymbolType.LITERAL) {
                DataType targetType = reference.valueType();
                if (reference instanceof DynamicReference) {
                    targetType = valueSymbol.valueType();
                }
                return castIfNeededOrFail(valueSymbol, targetType, context);
            }
            literal = (Literal) valueSymbol;

            if (reference instanceof DynamicReference) {
                if (reference.info().columnPolicy() != ColumnPolicy.IGNORED) {
                    // re-guess without strict to recognize timestamps
                    DataType<?> dataType = DataTypes.guessType(literal.value(), false);
                    validateInputType(dataType, reference.info().ident().columnIdent());
                    ((DynamicReference) reference).valueType(dataType);
                    literal = Literal.convert(literal, dataType); // need to update literal if the type changed
                } else {
                    ((DynamicReference) reference).valueType(literal.valueType());
                }
            } else {
                validateInputType(literal.valueType(), reference.info().ident().columnIdent());
                literal = Literal.convert(literal, reference.valueType());
            }
        } catch (ConversionException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    String.format("%s can not be cast to \'%s\'", SymbolFormatter.format(valueSymbol),
                            reference.valueType().getName()));
        }

        try {
            // 3. if reference is of type object - do special validation
            if (reference.info().type() == DataTypes.OBJECT) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) literal.value();
                if (value == null) {
                    return Literal.NULL;
                }
                literal = Literal.newLiteral(normalizeObjectValue(value, reference.info(), true));
            } else if (isObjectArray(reference.info().type())) {
                Object[] value = (Object[]) literal.value();
                if (value == null) {
                    return Literal.NULL;
                }
                literal = Literal.newLiteral(
                        reference.info().type(),
                        normalizeObjectArrayValue(value, reference.info(), true)
                );
            }
        } catch (ConversionException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    SymbolFormatter.format(
                            "\"%s\" has a type that can't be implicitly cast to that of \"%s\" (" + reference.valueType().getName() + ")",
                            literal,
                            reference
                    ));
        }
        return literal;
    }

    /**
     * validate input types to not be nested arrays/collection types
     *
     * @throws ColumnValidationException if input type is a nested array type
     */
    private void validateInputType(DataType dataType, ColumnIdent columnIdent) throws ColumnValidationException {
        if (dataType != null
                && DataTypes.isCollectionType(dataType)
                && DataTypes.isCollectionType(((CollectionType) dataType).innerType())) {
            throw new ColumnValidationException(columnIdent.sqlFqn(),
                    String.format(Locale.ENGLISH, "Invalid datatype '%s'", dataType));
        }
    }


    /**
     * normalize and validate the given value according to the given {@link io.crate.types.DataType}
     *
     * @param inputValue any {@link io.crate.analyze.symbol.Symbol} that evaluates to a Literal or Parameter
     * @param dataType   the type to convert this input to
     * @return a {@link io.crate.analyze.symbol.Literal} of type <code>dataType</code>
     */
    public Literal normalizeInputForType(Symbol inputValue, DataType dataType) {
        try {
            return Literal.convert(normalizer.normalize(inputValue), dataType);
        } catch (ConversionException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid value of type '%s'", inputValue.symbolType().name()));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeObjectValue(Map<String, Object> value, ReferenceInfo info, boolean forWrite) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            ColumnIdent nestedIdent = ColumnIdent.getChild(info.ident().columnIdent(), entry.getKey());
            TableInfo tableInfo = schemas.getTableInfo(info.ident().tableIdent());
            ReferenceInfo nestedInfo = tableInfo.getReferenceInfo(nestedIdent);
            if (nestedInfo == null) {
                if (info.columnPolicy() == ColumnPolicy.IGNORED) {
                    continue;
                }
                DynamicReference dynamicReference = null;
                if (tableInfo instanceof DocTableInfo){
                    dynamicReference = ((DocTableInfo)tableInfo).getDynamic(nestedIdent, forWrite);
                }
                if (dynamicReference == null) {
                    throw new ColumnUnknownException(nestedIdent.sqlFqn());
                }
                DataType type = DataTypes.guessType(entry.getValue(), false);
                if (type == null) {
                    throw new ColumnValidationException(info.ident().columnIdent().sqlFqn(), "Invalid value");
                }
                dynamicReference.valueType(type);
                nestedInfo = dynamicReference.info();
            } else {
                if (entry.getValue() == null) {
                    continue;
                }
            }
            if (nestedInfo.type() == DataTypes.OBJECT && entry.getValue() instanceof Map) {
                value.put(entry.getKey(), normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo, forWrite));
            } else if (isObjectArray(nestedInfo.type()) && entry.getValue() instanceof Object[]) {
                value.put(entry.getKey(), normalizeObjectArrayValue((Object[]) entry.getValue(), nestedInfo, forWrite));
            } else {
                value.put(entry.getKey(), normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
        return value;
    }

    private boolean isObjectArray(DataType type) {
        return type.id() == ArrayType.ID && ((ArrayType) type).innerType().id() == ObjectType.ID;
    }

    private Object[] normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo, boolean forWrite) {
        for (Object arrayItem : value) {
            Preconditions.checkArgument(arrayItem instanceof Map, "invalid value for object array type");
            // return value not used and replaced in value as arrayItem is a map that is mutated
            normalizeObjectValue((Map<String, Object>) arrayItem, arrayInfo, forWrite);
        }
        return value;
    }

    private Object normalizePrimitiveValue(Object primitiveValue, ReferenceInfo info) {
        try {
            if (info.type().equals(DataTypes.STRING) && primitiveValue instanceof String) {
                return primitiveValue;
            }
            return info.type().value(primitiveValue);
        } catch (Exception e) {
            throw new ColumnValidationException(info.ident().columnIdent().sqlFqn(),
                    String.format("Invalid %s",
                            info.type().getName()
                    )
            );
        }
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
                throw new UnsupportedOperationException("Function(DISTINCT x) does not accept more than one argument");
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
            functionInfo = getFunctionInfo(ident);
            arguments = outerArguments;
        } else {
            FunctionIdent ident = new FunctionIdent(node.getName().toString(), argumentTypes);
            functionInfo = getFunctionInfo(ident);
        }
        return context.allocateFunction(functionInfo, arguments);
    }


    public void resolveWritableFields(boolean value) {
        forWrite = value;
    }


    private static Symbol castIfNeededOrFail(Symbol symbolToCast, DataType targetType, ExpressionAnalysisContext context) {
        DataType sourceType = symbolToCast.valueType();
        if (sourceType.equals(targetType)) {
            return symbolToCast;
        }
        if (sourceType.isConvertableTo(targetType)) {
            // cast further below doesn't always fail because it might be lazy as it wraps functions/references inside a cast function.
            // -> Need to check isConvertableTo to fail eagerly if the cast won't work.
            try {
                return cast(symbolToCast, targetType, context, false);
            } catch (ConversionException e) {
                // exception is just thrown for literals, rest is evaluated lazy
                throw new IllegalArgumentException(String.format("%s cannot be cast to type %s",
                        SymbolFormatter.format(symbolToCast), targetType.getName()), e);
            }
        }
        throw new IllegalArgumentException(String.format("%s cannot be cast to type %s",
                SymbolFormatter.format(symbolToCast), targetType.getName()));
    }

    private static Symbol cast(Symbol sourceSymbol, DataType targetType, ExpressionAnalysisContext context,
                               boolean tryCast) {
        if (sourceSymbol.symbolType().isValueSymbol()) {
            return Literal.convert(sourceSymbol, targetType);
        }
        FunctionInfo functionInfo = CastFunctionResolver.functionInfo(sourceSymbol.valueType(), targetType, tryCast);
        return context.allocateFunction(functionInfo, Arrays.asList(sourceSymbol));
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
            } else if (!group2.isEmpty() && group3.isEmpty()){
                return null;
            }
            quoted.append(matcher.group(4));
            return quoted.toString();
        } else {
            return null;
        }
    }

    class InnerExpressionAnalyzer extends AstVisitor<Symbol, ExpressionAnalysisContext> {

        @Override
        protected Symbol visitNode(Node node, ExpressionAnalysisContext context) {
            throw new UnsupportedOperationException(String.format(
                    "Unsupported node %s", node));
        }

        @Override
        protected Symbol visitExpression(Expression node, ExpressionAnalysisContext context) {
            throw new UnsupportedOperationException(String.format(
                    "Unsupported expression %s", ExpressionFormatter.formatExpression(node)));
        }

        @Override
        protected Symbol visitCurrentTime(CurrentTime node, ExpressionAnalysisContext context) {
            if (!node.getType().equals(CurrentTime.Type.TIMESTAMP)) {
                visitExpression(node, context);
            }
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    Literal.newLiteral(node.getPrecision().or(CurrentTimestampFunction.DEFAULT_PRECISION))
            );
            return context.allocateCurrentTime(node, args, normalizer);
        }

        @Override
        protected Symbol visitFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
            return convertFunctionCall(node, context);
        }

        @Override
        protected Symbol visitCast(Cast node, ExpressionAnalysisContext context) {
            DataType returnType = DATA_TYPE_ANALYZER.process(node.getType(), null);
            return cast(process(node.getExpression(), context), returnType, context, false);
        }

        @Override
        protected Symbol visitTryCast(TryCast node, ExpressionAnalysisContext context) {
            DataType returnType = DATA_TYPE_ANALYZER.process(node.getType(), null);

            if (CastFunctionResolver.supportsExplicitConversion(returnType)) {
                try {
                    return cast(process(node.getExpression(), context), returnType, context, true);
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
            expression = castIfNeededOrFail(expression, DataTypes.TIMESTAMP, context);
            return context.allocateFunction(ExtractFunctions.functionInfo(node.getField()), Arrays.asList(expression));
        }

        @Override
        protected Symbol visitInPredicate(InPredicate node, ExpressionAnalysisContext context) {

            Symbol left = process(node.getValue(), context);

            DataType leftType = left.valueType();
            if (leftType.equals(DataTypes.UNDEFINED)) {
                // dynamic or null values cannot be queried, in scalar use-cases (system tables)
                // we currently have no dynamics
                return Literal.NULL;
            }

            List<Expression> expressions = ((InListExpression) node.getValueList()).getValues();

            boolean useAny = true;
            Set<Literal> literals = new HashSet<>(expressions.size());
            for (Expression expression : expressions) {
                Symbol arrayElement = process(expression, context);
                Literal literal;
                if (arrayElement instanceof Literal) {
                    literal = (Literal) arrayElement;
                } else {
                    Symbol normalized = normalize(arrayElement);
                    if (normalized instanceof Literal) {
                        literal = (Literal)normalized;
                    } else {
                        useAny = false;
                        break;
                    }
                }
                try {
                    literals.add(Literal.convert(literal, leftType));
                } catch (Throwable e) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH, "invalid IN LIST value %s. expected type '%s'",
                                    SymbolFormatter.format(literal),
                                    leftType.getName()));
                }
            }
            if (useAny){
                return context.allocateFunction(
                        new FunctionInfo(
                                new FunctionIdent(AnyEqOperator.NAME, Arrays.asList(leftType, new SetType(leftType))),
                                DataTypes.BOOLEAN
                        ),
                        Arrays.asList(left, Literal.implodeCollection(leftType, literals))
                );
            }

            Set<Function> comparisons = new HashSet<>(expressions.size());
            FunctionInfo eqInfo = new FunctionInfo(
                    new FunctionIdent(EqOperator.NAME, Arrays.asList(leftType, leftType)),
                    DataTypes.BOOLEAN);
            for (Expression expression : ((InListExpression) node.getValueList()).getValues()) {
                Symbol right = expression.accept(this, context);
                if (!right.valueType().equals(leftType)) {
                    if (right.valueType().isConvertableTo(leftType)) {
                        right = context.allocateFunction(CastFunctionResolver
                                .functionInfo(right.valueType(), leftType, false), Arrays.asList(right));
                    } else {
                        throw new IllegalArgumentException(
                                String.format(Locale.ENGLISH, "invalid IN LIST value %s. expected type '%s'",
                                        SymbolFormatter.format(right),
                                        leftType.getName()));
                    }
                }
                comparisons.add(context.allocateFunction(eqInfo, Arrays.asList(left, right)));
            }

            if (comparisons.size() == 1) {
                return comparisons.iterator().next();
            } else {
                Iterator<Function> iter = comparisons.iterator();
                Symbol result = iter.next();
                while (iter.hasNext()) {
                    result = context.allocateFunction(OrOperator.INFO, Arrays.asList(result, iter.next()));
                }
                return result;
            }
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
            SUBSCRIPT_VISITOR.process(node, subscriptContext);
            return resolveSubscriptSymbol(subscriptContext, context);
        }

        protected Symbol resolveSubscriptSymbol(SubscriptContext subscriptContext, ExpressionAnalysisContext context) {
            // TODO: support nested subscripts as soon as DataTypes.OBJECT elements can be typed
            Symbol subscriptSymbol;
            Expression subscriptExpression = subscriptContext.expression();
            if (subscriptContext.qName() != null && subscriptExpression == null) {
                subscriptSymbol = resolveQualifiedName(subscriptContext.qName(), subscriptContext.parts());
            } else if (subscriptExpression != null) {
                subscriptSymbol = subscriptExpression.accept(this, context);
            } else {
                throw new UnsupportedOperationException("Only references, function calls or array literals " +
                        "are valid subscript symbols");
            }
            assert subscriptSymbol != null : "subscriptSymbol must not be null";
            Integer index = subscriptContext.index();
            if (index != null) {
                // rewrite array access to subscript scalar
                FunctionIdent functionIdent = new FunctionIdent(SubscriptFunction.NAME,
                        ImmutableList.of(subscriptSymbol.valueType(), DataTypes.INTEGER));
                return context.allocateFunction(getFunctionInfo(functionIdent),
                        Arrays.asList(subscriptSymbol, newLiteral(index)));
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
                    throw new UnsupportedOperationException("Unsupported logical binary expression " + node.getType().name());
            }
            List<Symbol> arguments = new ArrayList<>(2);
            arguments.add(process(node.getLeft(), context));
            arguments.add(process(node.getRight(), context));
            return context.allocateFunction(functionInfo, arguments);
        }

        @Override
        protected Symbol visitNotExpression(NotExpression node, ExpressionAnalysisContext context) {
            Symbol argument = process(node.getValue(), context);

            DataType dataType = argument.valueType();
            if (!dataType.equals(DataTypes.BOOLEAN) && !dataType.equals(DataTypes.UNDEFINED)) {
                throw new IllegalArgumentException(String.format(
                        "Invalid argument of type \"%s\" passed to %s predicate. Argument must resolve to boolean or null",
                        dataType,
                        node
                ));
            }
            return new Function(NotPredicate.INFO, Arrays.asList(argument));
        }

        @Override
        protected Symbol visitComparisonExpression(ComparisonExpression node, ExpressionAnalysisContext context) {
            Symbol left = process(node.getLeft(), context);
            Symbol right = process(node.getRight(), context);

            if (left.valueType().equals(DataTypes.UNDEFINED) || right.valueType().equals(DataTypes.UNDEFINED)) {
                return Literal.NULL;
            }
            Comparison comparison = new Comparison(context, node.getType(), left, right);
            comparison.normalize(context);
            FunctionInfo info = getFunctionInfo(comparison.toFunctionIdent());
            return context.allocateFunction(info, comparison.arguments());
        }

        @Override
        public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, ExpressionAnalysisContext context) {
            if (node.quantifier().equals(ArrayComparisonExpression.Quantifier.ALL)) {
                throw new UnsupportedFeatureException("ALL is not supported");
            }

            Symbol rightSymbol = process(node.getRight(), context);
            Symbol leftSymbol = process(node.getLeft(), context);
            DataType rightType = rightSymbol.valueType();

            if (!DataTypes.isCollectionType(rightType)) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("invalid array expression: '%s'", rightSymbol));
            }
            DataType rightInnerType = ((CollectionType) rightType).innerType();
            if (rightInnerType.equals(DataTypes.OBJECT)) {
                throw new IllegalArgumentException("ANY on object arrays is not supported");
            }

            // always try to convert literal to reference type
            if (!rightInnerType.equals(leftSymbol.valueType())) {
                if (rightInnerType.isConvertableTo(leftSymbol.valueType())) {
                    if (rightSymbol.symbolType().isValueSymbol()) {
                        rightSymbol = Literal.convert(rightSymbol, new ArrayType(leftSymbol.valueType()));
                    } else {
                        leftSymbol = normalizeInputForType(leftSymbol, rightInnerType);
                    }
                } else {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "array expression of invalid type array(%s)", rightInnerType));
                }
            }

            ComparisonExpression.Type operationType = node.getType();
            String operatorName;
            operatorName = AnyOperator.OPERATOR_PREFIX + operationType.getValue();
            FunctionIdent functionIdent = new FunctionIdent(operatorName, Arrays.asList(leftSymbol.valueType(), rightSymbol.valueType()));
            FunctionInfo functionInfo = getFunctionInfo(functionIdent);
            return context.allocateFunction(functionInfo, Arrays.asList(leftSymbol, rightSymbol));
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
            DataType rightInnerType = ((CollectionType) rightType).innerType();

            // always try to convert literal to reference type
            if (rightInnerType.id() != StringType.ID) {
                if (rightInnerType.isConvertableTo(DataTypes.STRING)) {
                    if (rightSymbol.symbolType().isValueSymbol()) {
                        rightSymbol = Literal.convert(rightSymbol, new ArrayType(DataTypes.STRING));
                    } else {
                        leftSymbol = normalizeInputForType(leftSymbol, DataTypes.STRING);
                    }
                } else {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "array expression of invalid type array(%s)", rightInnerType));
                }
            }

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
            // catch null types, might be null or dynamic reference, which are both not supported
            if (expression.valueType().equals(DataTypes.UNDEFINED)) {
                return Literal.NULL;
            }
            expression = castIfNeededOrFail(expression, DataTypes.STRING, context);
            Symbol pattern = normalize(castIfNeededOrFail(process(node.getPattern(), context), DataTypes.STRING, context));
            assert pattern != null : "pattern must not be null";
            if (!pattern.symbolType().isValueSymbol()) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern must not be a reference.");
            }
            FunctionIdent functionIdent = FunctionIdent.of(LikeOperator.NAME, expression.valueType(), pattern.valueType());
            return context.allocateFunction(getFunctionInfo(functionIdent), Arrays.asList(expression, pattern));
        }

        @Override
        protected Symbol visitIsNullPredicate(IsNullPredicate node, ExpressionAnalysisContext context) {
            Symbol value = process(node.getValue(), context);

            // currently there will be no result for dynamic references, so return here
            if (!value.symbolType().isValueSymbol() && value.valueType().equals(DataTypes.UNDEFINED)) {
                return Literal.NULL;
            }

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
                return resolveQualifiedName(node.getName(), null);
            } catch (ColumnUnknownException exception) {
                if ((parameterContext.headerFlags() & SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT) == SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT) {
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

        protected Symbol resolveQualifiedName(QualifiedName qualifiedName, @Nullable List<String> path) {
            return fieldProvider.resolveField(qualifiedName, path, forWrite);
        }

        @Override
        protected Symbol visitBooleanLiteral(BooleanLiteral node, ExpressionAnalysisContext context) {
            return newLiteral(node.getValue());
        }

        @Override
        protected Symbol visitStringLiteral(StringLiteral node, ExpressionAnalysisContext context) {
            return newLiteral(node.getValue());
        }

        @Override
        protected Symbol visitDoubleLiteral(DoubleLiteral node, ExpressionAnalysisContext context) {
            return newLiteral(node.getValue());
        }

        @Override
        protected Symbol visitLongLiteral(LongLiteral node, ExpressionAnalysisContext context) {
            return newLiteral(node.getValue());
        }

        @Override
        protected Symbol visitNullLiteral(NullLiteral node, ExpressionAnalysisContext context) {
            return newLiteral(UndefinedType.INSTANCE, null);
        }

        @Override
        public Symbol visitArrayLiteral(ArrayLiteral node, ExpressionAnalysisContext context) {
            // TODO: support everything that is immediately evaluable as values
            return toArrayLiteral(node.values(), context);
        }

        private Literal toArrayLiteral(List<Expression> values, ExpressionAnalysisContext context) {
            if (values.isEmpty()) {
                return newLiteral(new ArrayType(UndefinedType.INSTANCE), new Object[0]);
            } else {
                DataType innerType = null;
                List<Literal> literals = new ArrayList<>(values.size());
                for (Expression e : values) {
                    Symbol arrayElement = process(e, context);
                    if (innerType == null || (innerType == DataTypes.UNDEFINED && !arrayElement.valueType().equals(innerType))) {
                        innerType = arrayElement.valueType();
                    } else if (!arrayElement.valueType().equals(innerType)) {
                        throw new IllegalArgumentException(String.format(
                                Locale.ENGLISH, "array element %s not of array item type %s", e, innerType));
                    }
                    literals.add(Literal.convert(arrayElement, innerType));
                }
                return Literal.implodeCollection(innerType, literals);
            }
        }

        @Override
        public Symbol visitObjectLiteral(ObjectLiteral node, ExpressionAnalysisContext context) {
            // TODO: support everything that is immediately evaluable as values
            Map<String, Object> values = new HashMap<>(node.values().size());
            for (Map.Entry<String, Expression> entry : node.values().entries()) {
                Object value;
                try {
                    value = ExpressionToObjectVisitor.convert(entry.getValue(), parameterContext.parameters());
                } catch (UnsupportedOperationException e) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "invalid object literal value '%s'",
                                    entry.getValue())
                    );
                }

                if (values.put(entry.getKey(), value) != null) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "key '%s' listed twice in object literal",
                                    entry.getKey()));
                }
            }
            return newLiteral(values);
        }

        @Override
        public Symbol visitParameterExpression(ParameterExpression node, ExpressionAnalysisContext context) {
            return parameterContext.getAsSymbol(node.index());
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate node, ExpressionAnalysisContext context) {
            Map<Field, Double> identBoostMap = new HashMap<>(node.idents().size());
            DataType columnType = null;
            for (MatchPredicateColumnIdent ident : node.idents()) {
                Symbol column = process(ident.columnIdent(), context);
                if (columnType == null) {
                    columnType = column.valueType();
                }
                Preconditions.checkArgument(
                        column instanceof Field,
                        SymbolFormatter.format("can only MATCH on columns, not on %s", column));
                Number boost = ExpressionToNumberVisitor.convert(ident.boost(), parameterContext.parameters());
                identBoostMap.put(((Field) column), boost == null ? null : boost.doubleValue());
            }
            assert columnType != null : "columnType must not be null";
            verifyTypesForMatch(identBoostMap.keySet(), columnType);

            Object queryTerm = ExpressionToObjectVisitor.convert(node.value(), parameterContext.parameters());
            queryTerm = columnType.value(queryTerm);
            String matchType = io.crate.operation.predicate.MatchPredicate.getMatchType(node.matchType(), columnType);
            Map<String, Object> options = MatchOptionsAnalysis.process(node.properties(), parameterContext.parameters());
            return new io.crate.analyze.symbol.MatchPredicate(identBoostMap, columnType, queryTerm, matchType, options);
        }

        private void verifyTypesForMatch(Iterable<? extends Symbol> columns, DataType columnType) {
            Preconditions.checkArgument(
                    io.crate.operation.predicate.MatchPredicate.SUPPORTED_TYPES.contains(columnType),
                    String.format("Can only use MATCH on columns of type STRING or GEO_SHAPE, not on '%s'", columnType));
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
        private final ExpressionAnalysisContext expressionAnalysisContext;

        private ComparisonExpression.Type comparisonExpressionType;
        private Symbol left;
        private Symbol right;
        private DataType leftType;
        private DataType rightType;
        private String operatorName;
        private FunctionIdent functionIdent = null;

        private Comparison(ExpressionAnalysisContext expressionAnalysisContext,
                           ComparisonExpression.Type comparisonExpressionType,
                           Symbol left,
                           Symbol right) {
            this.expressionAnalysisContext = expressionAnalysisContext;
            this.operatorName = "op_" + comparisonExpressionType.getValue();
            this.comparisonExpressionType = comparisonExpressionType;
            this.left = left;
            this.right = right;
            this.leftType = left.valueType();
            this.rightType = right.valueType();
        }

        void normalize(ExpressionAnalysisContext context) {
            swapIfNecessary();
            castTypes();
            rewriteNegatingOperators(context);
        }

        /**
         * swaps the comparison so that references are on the left side.
         * e.g.:
         * eq(2, name)  becomes  eq(name, 2)
         */
        private void swapIfNecessary() {
            if (!(left.symbolType().isValueSymbol() && (right instanceof Reference || right instanceof Field))) {
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
            right = castIfNeededOrFail(right, leftType, expressionAnalysisContext);
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
