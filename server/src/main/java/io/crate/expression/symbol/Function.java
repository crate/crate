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

package io.crate.expression.symbol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.ConversionException;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.operator.ExistsOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.ExtractFunctions;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.SubscriptObjectFunction;
import io.crate.expression.scalar.SubscriptRecordFunction;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.scalar.arithmetic.NegateFunctions;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.scalar.cast.TryCastFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemaFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemasFunction;
import io.crate.expression.scalar.timestamp.CurrentTimeFunction;
import io.crate.expression.scalar.timestamp.CurrentTimestampFunction;
import io.crate.expression.symbol.format.MatchPrinter;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class Function implements Symbol, Cloneable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Function.class);

    // POWER is not mapped to ^ to keep toString implementation printing 'power()'.
    private static final Map<String, String> ARITHMETIC_OPERATOR_MAPPING = Map.ofEntries(
        Map.entry(ArithmeticFunctions.Names.ADD, "+"),
        Map.entry(ArithmeticFunctions.Names.SUBTRACT, "-"),
        Map.entry(ArithmeticFunctions.Names.MULTIPLY, "*"),
        Map.entry(ArithmeticFunctions.Names.DIVIDE, "/"),
        Map.entry(ArithmeticFunctions.Names.MOD, "%"),
        Map.entry(ArithmeticFunctions.Names.MODULUS, "%")

    );

    private final List<Symbol> arguments;
    protected final DataType<?> returnType;
    protected final Signature signature;
    @Nullable
    protected final Symbol filter;

    public Function(StreamInput in) throws IOException {
        Signature generatedSignature = null;
        if (in.getVersion().before(Version.V_5_0_0)) {
            generatedSignature = Signature.readFromFunctionInfo(in);
        }
        if (in.getVersion().onOrAfter(Version.V_4_1_0)) {
            filter = Symbol.nullableFromStream(in);
        } else {
            filter = null;
        }
        arguments = List.copyOf(Symbols.fromStream(in));
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            if (in.getVersion().before(Version.V_5_0_0)) {
                in.readBoolean();
            }
            signature = new Signature(in);
            returnType = DataTypes.fromStream(in);
        } else {
            assert generatedSignature != null : "expecting a non-null generated signature";
            signature = generatedSignature;
            returnType = generatedSignature.getReturnType().createType();
        }
    }

    public Function(Signature signature, List<Symbol> arguments, DataType<?> returnType) {
        this(signature, arguments, returnType, null);
    }

    public Function(Signature signature, List<Symbol> arguments, DataType<?> returnType, @Nullable Symbol filter) {
        this.signature = signature;
        this.arguments = List.copyOf(arguments);
        this.returnType = returnType;
        this.filter = filter;
    }

    public List<Symbol> arguments() {
        return arguments;
    }

    public String name() {
        return signature.getName().name();
    }

    public Signature signature() {
        return signature;
    }

    @Nullable
    public Symbol filter() {
        return filter;
    }

    @Override
    public DataType<?> valueType() {
        return returnType;
    }

    @Override
    public boolean isDeterministic() {
        if (!signature.isDeterministic()) {
            return false;
        }
        for (var arg : arguments) {
            if (!arg.isDeterministic()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean any(Predicate<? super Symbol> predicate) {
        if (predicate.test(this)) {
            return true;
        }
        for (var arg : arguments) {
            if (arg.any(predicate)) {
                return true;
            }
        }
        if (filter != null) {
            return filter.any(predicate);
        }
        return false;
    }

    @Override
    public Symbol cast(DataType<?> targetType, CastMode... modes) {
        String name = signature.getName().name();
        if (targetType instanceof ArrayType<?> arrayType && name.equals(ArrayFunction.NAME)) {
            /* We treat _array(...) in a special way since it's a value constructor and no regular function
             * This allows us to do proper type inference for inserts/updates where there are assignments like
             *
             *      some_array = [?, ?]
             * or
             *      some_array = array_cat([?, ?], [1, 2])
             */
            return castArrayElements(arrayType, modes);
        } else {
            return Symbol.super.cast(targetType, modes);
        }
    }

    @Override
    public Symbol uncast() {
        if (isCast()) {
            return arguments.get(0);
        }
        return this;
    }

    private Symbol castArrayElements(ArrayType<?> targetType, CastMode... modes) {
        DataType<?> innerType = targetType.innerType();
        ArrayList<Symbol> newArgs = new ArrayList<>(arguments.size());
        for (Symbol arg : arguments) {
            try {
                newArgs.add(arg.cast(innerType, modes));
            } catch (ConversionException e) {
                throw new ConversionException(returnType, targetType);
            }
        }
        return new Function(signature, newArgs, targetType, null);
    }

    public boolean isCast() {
        return castMode() != null;
    }

    /**
     * @return {@link CastMode} if {@link #isCast()} is true, otherwise null
     **/
    @Nullable
    public CastMode castMode() {
        return switch (name()) {
            case ExplicitCastFunction.NAME -> CastMode.EXPLICIT;
            case ImplicitCastFunction.NAME -> CastMode.IMPLICIT;
            case TryCastFunction.NAME -> CastMode.TRY;
            default -> null;
        };
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + arguments.stream().mapToLong(Symbol::ramBytesUsed).sum()
            + returnType.ramBytesUsed()
            + (filter == null ? 0 : filter.ramBytesUsed())
            + signature.ramBytesUsed();
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FUNCTION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFunction(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_5_0_0)) {
            signature.writeAsFunctionInfo(out, Symbols.typeView(arguments));
        }
        if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
            Symbol.nullableToStream(filter, out);
        }
        Symbols.toStream(arguments, out);
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            if (out.getVersion().before(Version.V_5_0_0)) {
                out.writeBoolean(true);
            }
            signature.writeTo(out);
            DataTypes.toStream(returnType, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Function function = (Function) o;
        return Objects.equals(arguments, function.arguments) &&
               Objects.equals(signature, function.signature) &&
               Objects.equals(filter, function.filter);
    }

    @Override
    public int hashCode() {
        int result = arguments.hashCode();
        result = 31 * result + signature.hashCode();
        result = 31 * result + (filter == null ? 0 : filter.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        StringBuilder builder = new StringBuilder();
        String name = signature.getName().name();
        switch (name) {
            case MatchPredicate.NAME:
                MatchPrinter.printMatchPredicate(this, style, builder);
                break;

            case NegateFunctions.NAME:
                printNegate(builder, style);
                break;

            case SubscriptFunction.NAME:
            case SubscriptObjectFunction.NAME:
                printSubscriptFunction(builder, style);
                break;

            case SubscriptRecordFunction.NAME:
                printSubscriptRecord(builder, style);
                break;

            case ExistsOperator.NAME:
                builder.append("EXISTS ");
                builder.append(arguments.get(0).toString(style));
                break;

            case "current_user":
                builder.append("CURRENT_USER");
                break;

            case "session_user":
                builder.append("SESSION_USER");
                break;

            case CurrentSchemasFunction.NAME:
                builder.append(CurrentSchemasFunction.NAME);
                break;

            case CurrentSchemaFunction.NAME:
                builder.append(CurrentSchemaFunction.NAME);
                break;

            case IsNullPredicate.NAME:
                builder.append("(");
                builder.append(arguments.get(0).toString(style));
                builder.append(" IS NULL)");
                break;

            case NotPredicate.NAME:
                builder.append("(NOT ");
                builder.append(arguments.get(0).toString(style));
                builder.append(")");
                break;

            case CountAggregation.NAME:
                if (arguments.isEmpty()) {
                    builder.append("count(*)");
                    printFilter(builder, style);
                } else {
                    printFunctionWithParenthesis(builder, style);
                }
                break;

            case CurrentTimestampFunction.NAME:
                if (arguments.isEmpty()) {
                    builder.append("CURRENT_TIMESTAMP");
                } else {
                    printFunctionWithParenthesis(builder, style);
                }
                break;

            case CurrentTimeFunction.NAME:
                if (arguments.isEmpty()) {
                    builder.append("CURRENT_TIME");
                } else {
                    printFunctionWithParenthesis(builder, style);
                }
                break;

            case ArrayFunction.NAME:
                printArray(builder, style);
                break;

            default:
                if (AnyOperator.OPERATOR_NAMES.contains(name)) {
                    printAnyOperator(builder, style);
                } else if (isCast()) {
                    printCastFunction(builder, style);
                } else if (name.startsWith(Operator.PREFIX)) {
                    printOperator(builder, style, null);
                } else if (name.startsWith(ExtractFunctions.NAME_PREFIX)) {
                    printExtract(builder, style);
                } else {
                    String arithmeticOperator = ARITHMETIC_OPERATOR_MAPPING.get(name);
                    if (arithmeticOperator != null) {
                        printOperator(builder, style, arithmeticOperator);
                    } else {
                        printFunctionWithParenthesis(builder, style);
                    }
                }
        }
        return builder.toString();
    }

    private void printNegate(StringBuilder builder, Style style) {
        builder.append("- ");
        builder.append(arguments.get(0).toString(style));
    }

    private void printSubscriptRecord(StringBuilder builder, Style style) {
        builder.append("(");
        builder.append(arguments.get(0).toString(style));
        builder.append(").");
        builder.append(arguments.get(1).toString(style));
    }

    private void printArray(StringBuilder builder, Style style) {
        builder.append("[");
        int size = arguments.size();
        for (int i = 0; i < size; i++) {
            Symbol arg = arguments.get(i);
            builder.append(arg.toString(style));
            if (i + 1 < size) {
                builder.append(", ");
            }
        }
        builder.append("]");
    }

    private void printAnyOperator(StringBuilder builder, Style style) {
        String name = signature.getName().name();
        assert name.startsWith(AnyOperator.OPERATOR_PREFIX) : "function for printAnyOperator must start with any prefix";
        assert arguments.size() == 2 : "function's number of arguments must be 2";
        String operatorName = name.substring(4).replace('_', ' ').toUpperCase(Locale.ENGLISH);
        builder
            .append("(") // wrap operator in parens to ensure precedence
            .append(arguments.get(0).toString(style))
            .append(" ")
            .append(operatorName)
            .append(" ")
            .append("ANY(")
            .append(arguments.get(1).toString(style))
            .append("))");
    }

    private void printCastFunction(StringBuilder builder, Style style) {
        var name = signature.getName().name();
        assert arguments.size() == 2 : "Expecting 2 arguments for function " + name;
        if (name.equalsIgnoreCase(ImplicitCastFunction.NAME)) {
            builder.append(arguments().get(0).toString(style));
        } else {
            var targetType = arguments.get(1).valueType();
            builder
                .append(name)
                .append("(")
                .append(arguments().get(0).toString(style))
                .append(" AS ")
                .append(targetType.toString())
                .append(")");
        }
    }

    private void printExtract(StringBuilder builder, Style style) {
        String name = signature.getName().name();
        assert name.startsWith(ExtractFunctions.NAME_PREFIX) : "name of function passed to printExtract must start with extract_";
        String fieldName = name.substring(ExtractFunctions.NAME_PREFIX.length());
        builder.append("extract(")
            .append(fieldName)
            .append(" FROM ");
        builder.append(arguments.get(0).toString(style));
        builder.append(")");
    }

    private void printOperator(StringBuilder builder, Style style, String operator) {
        if (operator == null) {
            String name = signature.getName().name();
            assert name.startsWith(Operator.PREFIX);
            operator = name.substring(Operator.PREFIX.length()).toUpperCase(Locale.ENGLISH);
        }
        builder
            .append("(")
            .append(arguments.get(0).toString(style))
            .append(" ")
            .append(operator)
            .append(" ")
            .append(arguments.get(1).toString(style))
            .append(")");
    }

    private void printSubscriptFunction(StringBuilder builder, Style style) {
        Symbol base = arguments.get(0);
        if (base instanceof Reference ref && base.valueType() instanceof ArrayType && !ref.column().path().isEmpty()) {
            builder.append(ref.column().getRoot().quotedOutputName());
            builder.append("[");
            builder.append(arguments.get(1).toString(style));
            builder.append("]");
            ref.column().path().forEach(path -> builder.append("['").append(path).append("']"));
        } else {
            builder.append(base.toString(style));
            builder.append("[");
            builder.append(arguments.get(1).toString(style));
            builder.append("]");
        }
    }

    private void printFunctionWithParenthesis(StringBuilder builder, Style style) {
        FunctionName functionName = signature.getName();
        builder.append(functionName.displayName());
        builder.append("(");
        for (int i = 0; i < arguments.size(); i++) {
            Symbol argument = arguments.get(i);
            builder.append(argument.toString(style));
            if (i + 1 < arguments.size()) {
                builder.append(", ");
            }
        }
        builder.append(")");
        printFilter(builder, style);
    }

    private void printFilter(StringBuilder builder, Style style) {
        if (filter != null) {
            builder.append(" FILTER (WHERE ");
            builder.append(filter.toString(style));
            builder.append(")");
        }
    }
}
