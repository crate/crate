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

package io.crate.expression.symbol.format;

import io.crate.execution.engine.aggregation.impl.CountAggregation;
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
import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.expression.scalar.systeminformation.CurrentSchemaFunction;
import io.crate.expression.scalar.systeminformation.CurrentSchemasFunction;
import io.crate.expression.scalar.timestamp.CurrentTimestampFunction;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.LiteralValueFormatter;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.crate.expression.scalar.cast.CastFunction.CAST_SQL_NAME;
import static io.crate.expression.scalar.cast.CastFunction.TRY_CAST_SQL_NAME;
import static io.crate.expression.scalar.cast.CastFunctionResolver.TRY_CAST_PREFIX;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.ANY;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.COMMA;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.DOT;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.NULL_UPPER;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_CLOSE;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_OPEN;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.WS;

public final class SymbolPrinter {

    /**
     * format symbols in simple style and use the formatted symbols as {@link String#format(Locale, String, Object...)} arguments
     * for the given <code>messageTmpl</code>.
     */
    public static String format(String messageTmpl, Symbol... symbols) {
        Object[] formattedSymbols = new String[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            Symbol s = symbols[i];
            if (s == null) {
                formattedSymbols[i] = NULL_UPPER;
            } else {
                formattedSymbols[i] = printUnqualified(s);
            }
        }
        return String.format(Locale.ENGLISH, messageTmpl, formattedSymbols);
    }

    public enum Style {
        UNQUALIFIED,
        QUALIFIED
    }

    private static final Map<String, String> ARITHMETIC_OPERATOR_MAPPING = Map.ofEntries(
        Map.entry(ArithmeticFunctions.Names.ADD, "+"),
        Map.entry(ArithmeticFunctions.Names.SUBTRACT, "-"),
        Map.entry(ArithmeticFunctions.Names.MULTIPLY, "*"),
        Map.entry(ArithmeticFunctions.Names.DIVIDE, "/"),
        Map.entry(ArithmeticFunctions.Names.MOD, "%"),
        Map.entry(ArithmeticFunctions.Names.MODULUS, "%")
    );

    public static String printUnqualified(Symbol symbol) {
        return print(symbol, Style.UNQUALIFIED);
    }

    public static String printQualified(Symbol symbol) {
        return print(symbol, Style.QUALIFIED);
    }

    /**
     * format a symbol with the given style
     */
    private static String print(Symbol symbol, Style style) {
        SymbolPrintVisitor printVisitor = new SymbolPrintVisitor(style);
        symbol.accept(printVisitor, null);
        return printVisitor.builder.toString();
    }

    static final class SymbolPrintVisitor extends SymbolVisitor<Void, Void> {

        private final StringBuilder builder;
        private final Style style;

        private SymbolPrintVisitor(Style style) {
            this.builder = new StringBuilder();
            this.style = style;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Void context) {
            builder.append(symbol);
            return null;
        }

        @Override
        public Void visitAlias(AliasSymbol aliasSymbol, Void context) {
            aliasSymbol.symbol().accept(this, null);
            builder.append(" AS ");
            builder.append(aliasSymbol.alias());
            return null;
        }

        @Override
        public Void visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            return super.visitSelectSymbol(selectSymbol, context);
        }

        @Override
        public Void visitAggregation(Aggregation symbol, Void context) {
            builder.append(symbol.functionIdent().name()).append(PAREN_OPEN);
            printArgs(symbol.inputs());
            builder.append(PAREN_CLOSE);
            return null;
        }

        @Override
        public Void visitFunction(Function function, Void context) {
            FunctionIdent ident = function.info().ident();
            String name = ident.name();
            switch (name) {
                case MatchPredicate.NAME:
                    MatchPrinter.printMatchPredicate(function, builder, this);
                    break;

                case SubscriptFunction.NAME:
                case SubscriptObjectFunction.NAME:
                    printSubscriptFunction(function);
                    break;

                case SubscriptRecordFunction.NAME:
                    printSubscriptRecord(function);
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
                    function.arguments().get(0).accept(this, context);
                    builder.append(" IS NULL)");
                    break;

                case NotPredicate.NAME:
                    builder.append("(NOT ");
                    function.arguments().get(0).accept(this, context);
                    builder.append(")");
                    break;

                case CountAggregation.NAME:
                    if (function.arguments().isEmpty()) {
                        builder.append("count(*)");
                        printFilter(function.filter());
                    } else {
                        printFunctionWithParenthesis(function);
                    }
                    break;

                case CurrentTimestampFunction.NAME:
                    if (function.arguments().isEmpty()) {
                        builder.append("CURRENT_TIMESTAMP");
                    } else {
                        printFunctionWithParenthesis(function);
                    }
                    break;

                default:
                    if (name.startsWith(AnyOperator.OPERATOR_PREFIX)) {
                        printAnyOperator(function);
                    } else if (CastFunctionResolver.isCastFunction(name)) {
                        printCastFunction(function);
                    } else if (name.startsWith(Operator.PREFIX)) {
                        printOperator(function, null);
                    } else if (name.startsWith(ExtractFunctions.NAME_PREFIX)) {
                        printExtract(function);
                    } else {
                        String arithmeticOperator = ARITHMETIC_OPERATOR_MAPPING.get(name);
                        if (arithmeticOperator != null) {
                            printOperator(function, arithmeticOperator);
                        } else {
                            printFunctionWithParenthesis(function);
                        }
                    }
            }
            return null;
        }

        private void printSubscriptRecord(Function function) {
            builder.append("(");
            function.arguments().get(0).accept(this, null);
            builder.append(").");
            function.arguments().get(1).accept(this, null);
        }

        private void printExtract(Function function) {
            String name = function.info().ident().name();
            assert name.startsWith(ExtractFunctions.NAME_PREFIX) : "name of function passed to printExtract must start with extract_";
            String fieldName = name.substring(ExtractFunctions.NAME_PREFIX.length());
            builder.append("extract(")
                .append(fieldName)
                .append(" FROM ");
            function.arguments().get(0).accept(this, null);
            builder.append(")");
        }

        public void printFunctionWithParenthesis(Function function) {
            FunctionName functionName = function.info().ident().fqnName();
            String schema = functionName.schema();
            if (style == Style.QUALIFIED && schema != null) {
                builder.append(schema).append(DOT);
            }
            builder
                .append(functionName.name())
                .append("(");
            printArgs(function.arguments())
                .append(")");
            printFilter(function.filter());
        }

        public void printFilter(@Nullable Symbol filter) {
            if (filter != null) {
                builder.append(" FILTER (WHERE ");
                filter.accept(this, null);
                builder.append(")");
            }
        }

        private void printOperator(Function function, @Nullable String operator) {
            if (operator == null) {
                String name = function.info().ident().name();
                assert name.startsWith(Operator.PREFIX);
                operator = name.substring(Operator.PREFIX.length()).toUpperCase(Locale.ENGLISH);
            }
            builder.append("(");
            List<Symbol> arguments = function.arguments();
            arguments.get(0).accept(this, null);
            builder
                .append(WS)
                .append(operator)
                .append(WS);
            arguments.get(1).accept(this, null);
            builder.append(")");
        }

        private void printCastFunction(Function function) {
            String prefix = function.info().ident().name().startsWith(TRY_CAST_PREFIX)
                ? TRY_CAST_SQL_NAME
                : CAST_SQL_NAME;
            final String asTypeName;
            DataType<?> dataType = function.valueType();
            if (DataTypes.isArray(dataType)) {
                ArrayType<?> arrayType = ((ArrayType<?>) dataType);
                asTypeName = " AS "
                             + ArrayType.NAME
                             + PAREN_OPEN
                             + arrayType.innerType().getName()
                             + PAREN_CLOSE;
            } else {
                asTypeName = " AS " + dataType.getName();
            }
            builder.append(prefix)
                .append("(");
            function.arguments().get(0).accept(this, null);
            builder
                .append(asTypeName)
                .append(")");
        }

        private void printAnyOperator(Function function) {
            String name = function.info().ident().name();
            assert name.startsWith(AnyOperator.OPERATOR_PREFIX) : "function for printAnyOperator must start with any prefix";
            List<Symbol> args = function.arguments();
            assert args.size() == 2 : "function's number of arguments must be 2";
            builder.append(PAREN_OPEN); // wrap operator in parens to ensure precedence
            args.get(0).accept(this, null);
            String operatorName = name.substring(4).replace('_', ' ').toUpperCase(Locale.ENGLISH);
            builder
                .append(WS)
                .append(operatorName)
                .append(WS);

            builder.append(ANY).append(PAREN_OPEN);
            args.get(1).accept(this, null);
            builder.append(PAREN_CLOSE)
                .append(PAREN_CLOSE);
        }

        private void printSubscriptFunction(Function function) {
            List<Symbol> arguments = function.arguments();
            Symbol base = arguments.get(0);
            if (base instanceof Reference && base.valueType() instanceof ArrayType
                && ((Reference) base).column().path().size() > 0) {

                Reference firstArgument = (Reference) base;
                builder.append(firstArgument.column().name());
                builder.append("[");
                arguments.get(1).accept(this, null);
                builder.append("]");
                builder.append("['");
                builder.append(firstArgument.column().path().get(0));
                builder.append("']");
            } else {
                base.accept(this, null);
                builder.append("[");
                arguments.get(1).accept(this, null);
                builder.append("]");
            }
        }

        @Override
        public Void visitWindowFunction(WindowFunction symbol, Void context) {
            return visitFunction(symbol, context);
        }


        @Override
        public Void visitReference(Reference symbol, Void context) {
            if (style == Style.QUALIFIED && !isTableFunctionReference(symbol)) {
                builder.append(symbol.ident().tableIdent().sqlFqn())
                    .append(DOT);
            }
            builder.append(symbol.column().quotedOutputName());
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, Void context) {
            return visitReference(symbol, context);
        }

        @Override
        public Void visitField(ScopedSymbol field, Void context) {
            if (style == Style.QUALIFIED) {
                builder
                    .append(field.relation().toString())
                    .append(DOT);
            }
            builder.append(field.column().quotedOutputName());
            return null;
        }

        @Override
        public Void visitInputColumn(InputColumn inputColumn, Void context) {
            builder.append("INPUT(")
                .append(inputColumn.index())
                .append(")");
            return null;
        }

        @Override
        public Void visitFetchReference(FetchReference fetchReference, Void context) {
            builder.append("FETCH(");
            fetchReference.fetchId().accept(this, context);
            builder.append(", ");
            fetchReference.ref().accept(this, context);
            builder.append(")");
            return null;
        }

        @Override
        public Void visitLiteral(Literal symbol, Void context) {
            LiteralValueFormatter.format(symbol.value(), builder);
            return null;
        }

        private StringBuilder printArgs(List<Symbol> args) {
            for (int i = 0, size = args.size(); i < size; i++) {
                args.get(i).accept(this, null);
                if (i < size - 1) {
                    builder.append(COMMA).append(WS);
                }
            }
            return builder;
        }

        private static boolean isTableFunctionReference(Reference reference) {
            RelationName relationName = reference.ident().tableIdent();
            return "".equals(relationName.schema());
        }
    }

    public static class Strings {
        public static final String PAREN_OPEN = "(";
        public static final String PAREN_CLOSE = ")";
        public static final String COMMA = ",";
        public static final String NULL_UPPER = "NULL";
        public static final String WS = " ";
        public static final String DOT = ".";
        public static final String ANY = "ANY";
    }
}
