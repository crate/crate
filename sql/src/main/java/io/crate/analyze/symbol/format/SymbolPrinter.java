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

package io.crate.analyze.symbol.format;

import io.crate.analyze.relations.RelationPrinter;
import io.crate.analyze.symbol.*;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.operation.operator.any.AnyOperator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

import static io.crate.analyze.symbol.format.SymbolPrinter.Strings.*;

@Singleton
public class SymbolPrinter {

    public static final SymbolPrinter INSTANCE = new SymbolPrinter(null);

    private static final FunctionFormatSpec SIMPLE_FUNCTION_FORMAT_SPEC = new FunctionFormatSpec() {
        @Override
        public boolean formatArgs(Function function) {
            return true;
        }

        @Override
        public String beforeArgs(Function function) {
            return function.info().ident().name() + PAREN_OPEN;
        }

        @Override
        public String afterArgs(Function function) {
            return PAREN_CLOSE;
        }
    };

    private static final OperatorFormatSpec SIMPLE_OPERATOR_FORMAT_SPEC = new OperatorFormatSpec() {
        @Override
        public String operator(Function function) {
            assert function.info().ident().name().startsWith("op_") :
                "function.info().ident().name() must start with 'op_'";
            return function.info().ident().name().substring(3).toUpperCase(Locale.ENGLISH);
        }
    };

    public static final com.google.common.base.Function<? super Symbol, String> FUNCTION = new com.google.common.base.Function<Symbol, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Symbol input) {
            return input == null ? null : INSTANCE.printSimple(input);
        }
    };

    public enum Style {
        SIMPLE(SymbolPrinterContext.DEFAULT_MAX_DEPTH, SymbolPrinterContext.DEFAULT_FULL_QUALIFIED, SymbolPrinterContext.DEFAULT_FAIL_IF_MAX_DEPTH_REACHED),
        PARSEABLE(100, true, true),
        FULL_QUALIFIED(SymbolPrinterContext.DEFAULT_MAX_DEPTH, true, false),
        PARSEABLE_NOT_QUALIFIED(100, false, true);

        private final int maxDepth;
        private final boolean fullQualified;
        private final boolean failIfMaxDepthReached;

        Style(int maxDepth, boolean fullQualified, boolean failIfMaxDepthReached) {
            this.maxDepth = maxDepth;
            this.fullQualified = fullQualified;
            this.failIfMaxDepthReached = failIfMaxDepthReached;
        }

        SymbolPrinterContext context() {
            return new SymbolPrinterContext(maxDepth, fullQualified, failIfMaxDepthReached);
        }
    }

    private final SymbolPrintVisitor symbolPrintVisitor;

    @Inject
    public SymbolPrinter(@Nullable Functions functions) {
        this.symbolPrintVisitor = new SymbolPrintVisitor(functions);
    }

    public String printSimple(Symbol symbol) {
        return print(symbol, Style.SIMPLE);
    }

    public String printFullQualified(Symbol symbol) {
        return print(symbol, Style.FULL_QUALIFIED);
    }

    /**
     * format the given symbol
     *
     * @param symbol        the symbol to format
     * @param maxDepth      the max depth to print, if maxDepth is reached, "..." is printed for the rest
     * @param fullQualified if references should be fully qualified (contain schema and table name)
     */
    public String print(Symbol symbol, int maxDepth, boolean fullQualified, boolean failIfMaxDepthReached) {
        SymbolPrinterContext context = new SymbolPrinterContext(maxDepth, fullQualified, failIfMaxDepthReached);
        symbolPrintVisitor.process(symbol, context);
        return context.formatted();
    }

    /**
     * format a symbol with the given style
     */
    public String print(Symbol symbol, Style formatStyle) {
        SymbolPrinterContext context = formatStyle.context();
        symbolPrintVisitor.process(symbol, context);
        return context.formatted();
    }

    private static final class SymbolPrintVisitor extends SymbolVisitor<SymbolPrinterContext, Void> {

        @Nullable
        private final Functions functions;

        private SymbolPrintVisitor(@Nullable Functions functions) {
            this.functions = functions;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }
            context.builder.append(symbol.toString());
            return null;
        }

        @Override
        public Void visitAggregation(Aggregation symbol, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

            context.builder.append(symbol.functionIdent().name()).append(PAREN_OPEN);
            printArgs(symbol.inputs(), context);
            context.builder.append(PAREN_CLOSE);
            return null;
        }

        @Override
        public Void visitFunction(Function function, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

            // handle special functions
            String functionName = function.info().ident().name();
            if (functionName.startsWith(AnyOperator.OPERATOR_PREFIX)) {
                printAnyOperator(function, context);
            } else {
                printGenericFunction(function, context);
            }
            return null;
        }

        private void printGenericFunction(Function function, SymbolPrinterContext context) {
            FunctionFormatSpec functionFormatSpec = null;
            OperatorFormatSpec operatorFormatSpec = null;

            if (functions != null) {
                FunctionImplementation impl = functions.get(function.info().ident());
                if (impl instanceof FunctionFormatSpec) {
                    functionFormatSpec = (FunctionFormatSpec) impl;
                } else if (impl instanceof OperatorFormatSpec) {
                    operatorFormatSpec = (OperatorFormatSpec) impl;
                }
            } else if (function.info().ident().name().startsWith("op_")) {
                operatorFormatSpec = SIMPLE_OPERATOR_FORMAT_SPEC;
            }
            if (operatorFormatSpec != null) {
                printOperator(function, operatorFormatSpec, context);
            } else {
                if (functionFormatSpec == null) {
                    functionFormatSpec = SIMPLE_FUNCTION_FORMAT_SPEC;
                }
                printFunction(function, functionFormatSpec, context);
            }
        }

        private void printAnyOperator(Function function, SymbolPrinterContext context) {

            List<Symbol> args = function.arguments();
            assert args.size() == 2 : "function's number of arguments must be 2";
            context.builder.append(PAREN_OPEN); // wrap operator in parens to ensure precedence
            context.down();
            process(args.get(0), context);
            context.up();

            // print operator
            String operatorName = anyOperatorName(function.info().ident().name());
            context.builder
                .append(WS)
                .append(operatorName)
                .append(WS);

            context.builder.append(ANY).append(PAREN_OPEN);
            context.down();
            process(args.get(1), context);
            context.up();
            context.builder.append(PAREN_CLOSE)
                .append(PAREN_CLOSE);
        }

        private String anyOperatorName(String functionName) {
            // handles NOT_LIKE -> NOT LIKE
            return functionName.substring(4).replace('_', ' ').toUpperCase(Locale.ENGLISH);
        }

        @Override
        public Void visitReference(Reference symbol, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

            if (context.isFullQualified()) {
                context.builder.append(symbol.ident().tableIdent().sqlFqn())
                    .append(DOT);
            }
            context.builder.append(symbol.ident().columnIdent().quotedOutputName());
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, SymbolPrinterContext context) {
            return visitReference(symbol, context);
        }

        @Override
        public Void visitField(Field field, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

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
        public Void visitRelationColumn(RelationColumn relationColumn, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

            context.builder.append("RELCOL(")
                .append(relationColumn.relationName())
                .append(", ")
                .append(relationColumn.index())
                .append(")");
            return null;
        }

        @Override
        public Void visitInputColumn(InputColumn inputColumn, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }
            context.builder.append("INPUT(")
                .append(inputColumn.index())
                .append(")");
            return null;
        }

        @Override
        public Void visitFetchReference(FetchReference fetchReference, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

            context.builder.append("FETCH(");
            process(fetchReference.fetchId(), context);
            context.builder.append(", ");
            process(fetchReference.ref(), context);
            context.builder.append(")");
            return null;
        }

        @Override
        public Void visitLiteral(Literal symbol, SymbolPrinterContext context) {
            if (context.verifyMaxDepthReached()) {
                return null;
            }

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
                    context.down();
                    function.arguments().get(0).accept(this, context);
                    context.up();
                    break;
                case 2:
                    context.down();
                    function.arguments().get(0).accept(this, context);
                    context.up();
                    context.builder.append(WS)
                        .append(formatter.operator(function))
                        .append(WS);
                    context.down();
                    function.arguments().get(1).accept(this, context);
                    context.up();
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
            context.down();
            try {
                for (int i = 0, size = args.size(); i < size; i++) {
                    args.get(i).accept(this, context);
                    if (i < size - 1) {
                        context.builder.append(COMMA).append(WS);
                    }
                }
            } finally {
                context.up();
            }
        }
    }

    public static class Strings {
        public static final String PAREN_OPEN = "(";
        public static final String PAREN_CLOSE = ")";
        public static final String COMMA = ",";
        public static final String ELLIPSIS = "...";
        public static final String NULL_LOWER = "null";
        public static final String WS = " ";
        public static final String DOT = ".";
        public static final String ANY = "ANY";
    }
}
