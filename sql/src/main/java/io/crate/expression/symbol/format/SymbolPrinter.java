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

import io.crate.analyze.SQLPrinter;
import io.crate.analyze.relations.RelationPrinter;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.LiteralValueFormatter;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.types.ArrayType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

import static io.crate.expression.symbol.format.SymbolPrinter.Strings.ANY;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.COMMA;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.DOT;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_CLOSE;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_OPEN;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.WS;

@Singleton
public final class SymbolPrinter {

    public static final SymbolPrinter INSTANCE = new SymbolPrinter(null);

    private static final OperatorFormatSpec SIMPLE_OPERATOR_FORMAT_SPEC = function -> {
        assert function.info().ident().name().startsWith("op_") :
            "function.info().ident().name() must start with 'op_'";
        return function.info().ident().name().substring(3).toUpperCase(Locale.ENGLISH);
    };

    public enum Style {
        UNQUALIFIED,
        QUALIFIED;

        SymbolPrinterContext createNewContext(@Nullable SQLPrinter.Visitor sqlPrinterVisitor) {
            return new SymbolPrinterContext(this, sqlPrinterVisitor);
        }
    }

    private final SymbolPrintVisitor symbolPrintVisitor;
    @Nullable
    private SQLPrinter.Visitor sqlPrinterVisitor;

    @Inject
    public SymbolPrinter(@Nullable Functions functions) {
        this.symbolPrintVisitor = new SymbolPrintVisitor(functions);
    }

    public String printUnqualified(Symbol symbol) {
        return print(symbol, Style.UNQUALIFIED);
    }

    public String printQualified(Symbol symbol) {
        return print(symbol, Style.QUALIFIED);
    }

    /**
     * format a symbol with the given style
     */
    private String print(Symbol symbol, Style style) {
        SymbolPrinterContext context = style.createNewContext(sqlPrinterVisitor);
        symbolPrintVisitor.process(symbol, context);
        return context.formatted();
    }

    public void registerSqlPrinterVisitor(SQLPrinter.Visitor sqlPrinter) {
        this.sqlPrinterVisitor = sqlPrinter;
    }

    static final class SymbolPrintVisitor extends SymbolVisitor<SymbolPrinterContext, Void> {

        @Nullable
        private final Functions functions;

        private SymbolPrintVisitor(@Nullable Functions functions) {
            this.functions = functions;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, SymbolPrinterContext context) {
            context.builder.append(symbol.toString());
            return null;
        }

        @Override
        public Void visitSelectSymbol(SelectSymbol selectSymbol, SymbolPrinterContext context) {
            if (context.sqlPrinterExists()) {
                context.builder.append("(");
                context.processWithSqlPrinter(selectSymbol.relation());
                context.builder.append(")");
                return null;
            }
            return super.visitSelectSymbol(selectSymbol, context);
        }

        @Override
        public Void visitAggregation(Aggregation symbol, SymbolPrinterContext context) {

            context.builder.append(symbol.functionIdent().name()).append(PAREN_OPEN);
            printArgs(symbol.inputs(), context);
            context.builder.append(PAREN_CLOSE);
            return null;
        }

        @Override
        public Void visitFunction(Function function, SymbolPrinterContext context) {
            String functionName = function.info().ident().name();
            if (functionName.startsWith(AnyOperator.OPERATOR_PREFIX)) {
                printAnyOperator(function, context);
            } else if (functionName.equals(MatchPredicate.NAME)) {
                printMatchPredicate(function, context);
            } else if (functionName.equals(SubscriptFunction.NAME)) {
                printSubscriptFunction(function, context);
            } else {
                printGenericFunction(function, context);
            }
            return null;
        }

        private void printGenericFunction(Function function, SymbolPrinterContext context) {
            FunctionFormatSpec functionFormatSpec = null;
            OperatorFormatSpec operatorFormatSpec = null;

            FunctionIdent ident = function.info().ident();
            if (functions != null) {
                FunctionImplementation impl = functions.getQualified(ident);
                if (impl instanceof FunctionFormatSpec) {
                    functionFormatSpec = (FunctionFormatSpec) impl;
                } else if (impl instanceof OperatorFormatSpec) {
                    operatorFormatSpec = (OperatorFormatSpec) impl;
                }
            } else if (ident.name().startsWith("op_")) {
                operatorFormatSpec = SIMPLE_OPERATOR_FORMAT_SPEC;
            }
            if (operatorFormatSpec != null) {
                printOperator(function, operatorFormatSpec, context);
            } else {
                if (functionFormatSpec == null) {
                    functionFormatSpec = FunctionFormatSpec.NAME_PARENTHESISED_ARGS;
                }
                printFunction(function, functionFormatSpec, context);
            }
        }

        private void printAnyOperator(Function function, SymbolPrinterContext context) {

            List<Symbol> args = function.arguments();
            assert args.size() == 2 : "function's number of arguments must be 2";
            context.builder.append(PAREN_OPEN); // wrap operator in parens to ensure precedence
            process(args.get(0), context);

            // print operator
            String operatorName = anyOperatorName(function.info().ident().name());
            context.builder
                .append(WS)
                .append(operatorName)
                .append(WS);

            context.builder.append(ANY).append(PAREN_OPEN);
            process(args.get(1), context);
            context.builder.append(PAREN_CLOSE)
                .append(PAREN_CLOSE);
        }

        private String anyOperatorName(String functionName) {
            // handles NOT_LIKE -> NOT LIKE
            return functionName.substring(4).replace('_', ' ').toUpperCase(Locale.ENGLISH);
        }

        private void printMatchPredicate(Function matchPredicate, SymbolPrinterContext context) {
            MatchPrinter.printMatchPredicate(matchPredicate, context, this);
        }

        private void printSubscriptFunction(Function function, SymbolPrinterContext context) {
            List<Symbol> arguments = function.arguments();
            if (arguments.get(0) instanceof Reference &&
                    arguments.get(0).valueType() instanceof ArrayType &&
                    ((Reference) arguments.get(0)).ident().columnIdent().path().size() > 0) {
                Reference firstArgument = (Reference) arguments.get(0);
                context.builder.append(firstArgument.ident().columnIdent().name());
                context.builder.append("[");
                process(arguments.get(1), context);
                context.builder.append("]");
                context.builder.append("['");
                context.builder.append(firstArgument.ident().columnIdent().path().get(0));
                context.builder.append("']");
            } else {
                process(arguments.get(0), context);
                context.builder.append("[");
                process(arguments.get(1), context);
                context.builder.append("]");
            }
        }

        @Override
        public Void visitReference(Reference symbol, SymbolPrinterContext context) {
            if (context.isFullQualified()) {
                context.builder.append(symbol.ident().tableIdent().sqlFqn())
                    .append(DOT);
            }
            context.builder.append(symbol.column().quotedOutputName());
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, SymbolPrinterContext context) {
            return visitReference(symbol, context);
        }

        @Override
        public Void visitField(Field field, SymbolPrinterContext context) {
            if (context.isFullQualified()) {
                context.builder.append(RelationPrinter.INSTANCE.process(field.relation(), null))
                    .append(DOT);
            }
            if (field.path() instanceof ColumnIdent) {
                context.builder.append(((ColumnIdent) field.path()).quotedOutputName());
            } else {
                context.builder.append(field.path().outputName());
            }
            return null;
        }

        @Override
        public Void visitInputColumn(InputColumn inputColumn, SymbolPrinterContext context) {
            context.builder.append("INPUT(")
                .append(inputColumn.index())
                .append(")");
            return null;
        }

        @Override
        public Void visitFetchReference(FetchReference fetchReference, SymbolPrinterContext context) {
            context.builder.append("FETCH(");
            process(fetchReference.fetchId(), context);
            context.builder.append(", ");
            process(fetchReference.ref(), context);
            context.builder.append(")");
            return null;
        }

        @Override
        public Void visitLiteral(Literal symbol, SymbolPrinterContext context) {
            LiteralValueFormatter.INSTANCE.format(symbol.value(), context.builder);
            return null;
        }

        private void printOperator(Function function, OperatorFormatSpec formatter, SymbolPrinterContext context) {
            int numArgs = function.arguments().size();
            context.builder.append(PAREN_OPEN);
            switch (numArgs) {
                case 1:
                    context.builder.append(formatter.operator(function))
                        .append(" ");
                    function.arguments().get(0).accept(this, context);
                    break;
                case 2:
                    function.arguments().get(0).accept(this, context);
                    context.builder.append(WS)
                        .append(formatter.operator(function))
                        .append(WS);
                    function.arguments().get(1).accept(this, context);
                    break;
                default:
                    throw new UnsupportedOperationException("cannot format operators with more than 2 operands");
            }
            context.builder.append(PAREN_CLOSE);
        }

        private void printFunction(Function function, FunctionFormatSpec formatter, SymbolPrinterContext context) {

            context.builder.append(formatter.beforeArgs(function));
            if (formatter.formatArgs(function)) {
                printArgs(function.arguments(), context);
            }
            context.builder.append(formatter.afterArgs(function));
        }

        private void printArgs(List<Symbol> args, SymbolPrinterContext context) {
            for (int i = 0, size = args.size(); i < size; i++) {
                args.get(i).accept(this, context);
                if (i < size - 1) {
                    context.builder.append(COMMA).append(WS);
                }
            }
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
