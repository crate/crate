/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.symbol;

import io.crate.analyze.QueriedTable;
import io.crate.analyze.relations.*;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

import static io.crate.analyze.symbol.SymbolFormatter.Strings.*;

@Singleton
public class SymbolFormatter extends SymbolVisitor<SymbolFormatter.Context, Void> {


    public static class MaxDepthReachedException extends RuntimeException {

        public MaxDepthReachedException(int maxDepth) {
            super(String.format(Locale.ENGLISH, "max depth of %s reached while traversing symbol", maxDepth));
        }
    }

    public static final com.google.common.base.Function<Symbol, String> SYMBOL_FORMAT_FUNCTION = new com.google.common.base.Function<Symbol, String>() {

        @Nullable
        @Override
        public String apply(@Nullable Symbol input) {
            if (input == null) {
                return null;
            }

            return INSTANCE.formatSimple(input);
        }
    };

    public enum Style {
        SIMPLE(Context.DEFAULT_MAX_DEPTH, Context.DEFAULT_FULL_QUALIFIED, Context.DEFAULT_FAIL_IF_MAX_DEPTH_REACHED),
        DEFAULT(Context.DEFAULT_MAX_DEPTH, Context.DEFAULT_FULL_QUALIFIED, Context.DEFAULT_FAIL_IF_MAX_DEPTH_REACHED),
        PARSEABLE(100, true, true),
        FULL_QUALIFIED(Context.DEFAULT_MAX_DEPTH, true, false),
        PARSEABLE_NOT_QUALIFIED(100, false, true);

        private final int maxDepth;
        private final boolean fullQualified;
        private final boolean failIfMaxDepthReached;

        private Style(int maxDepth, boolean fullQualified, boolean failIfMaxDepthReached) {
            this.maxDepth = maxDepth;
            this.fullQualified = fullQualified;
            this.failIfMaxDepthReached = failIfMaxDepthReached;
        }

        Context context() {
            return new Context(maxDepth, fullQualified, failIfMaxDepthReached);
        }
    }

    public static class Context {

        public static final int DEFAULT_MAX_DEPTH = 50;
        public static final boolean DEFAULT_FAIL_IF_MAX_DEPTH_REACHED = false;
        public static final boolean DEFAULT_FULL_QUALIFIED = false;
        private final StringBuilder builder = new StringBuilder();
        private final int maxDepth;
        private final boolean fullQualified;
        private final boolean failIfMaxDepthReached;

        private int depth = 0;

        public Context(int maxDepth, boolean fullQualified, boolean failIfMaxDepthReached) {
            this.maxDepth = maxDepth;
            this.fullQualified = fullQualified;
            this.failIfMaxDepthReached = failIfMaxDepthReached;
        }

        public Context down() {
            depth++;
            return this;
        }

        public Context up() {
            depth--;
            return this;
        }

        boolean verifyMaxDepthReached() {
            if (maxDepthReached()) {
                if (failIfMaxDepthReached) {
                    throw new MaxDepthReachedException(maxDepth);
                } else {
                    builder.append(ELLIPSIS);
                    return true;
                }
            }
            return false;
        }
        boolean maxDepthReached() {
            return depth >= maxDepth;
        }

        String formatted() {
            return builder.toString();
        }

        public boolean isFullQualified() {
            return fullQualified;
        }
    }

    public static final SymbolFormatter INSTANCE = new SymbolFormatter(null);
    private static final SimpleFunctionFormatter SIMPLE_FUNCTION_FORMATTER = new SimpleFunctionFormatter();

    @Nullable
    private final Functions functions;

    @Inject
    public SymbolFormatter(@Nullable Functions functions) {
        this.functions = functions;
    }

    /**
     * format a symbol in simple style and use the formatted symbols as {@link String#format(Locale, String, Object...)} arguments
     * for the given <code>messageTmpl</code>.
     */
    public static String formatTmpl(String messageTmpl, Symbol ... symbols) {
        Object[] formattedSymbols = new String[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            Symbol s = symbols[i];
            if (s == null) {
                formattedSymbols[i] = NULL_LOWER;
            } else {
                formattedSymbols[i] = INSTANCE.formatSimple(s);
            }
        }
        return String.format(Locale.ENGLISH, messageTmpl, formattedSymbols);
    }


    /**
     * format a symbol with the given style
     */
    public String format(Symbol symbol, Style formatStyle) {
        Context context = formatStyle.context();
        process(symbol, context);
        return context.formatted();
    }

    /**
     * format a symbol to a string that is guaranteed to be parseable
     * or throw an exception if the max depth is reached.
     */
    public String formatParseable(Symbol symbol) {
        return format(symbol, Style.PARSEABLE);
    }

    public String formatSimple(Symbol symbol) {
        return format(symbol, Style.SIMPLE);
    }

    public String formatFullQualified(Symbol symbol) {
        return format(symbol, Style.FULL_QUALIFIED);
    }

    /**
     * format the given symbol
     * @param symbol the symbol to format
     * @param maxDepth the max depth to print, if maxDepth is reached, "..." is printed for the rest
     * @param fullQualified if references should be fully qualified (contain schema and table name)
     */
    public String format(Symbol symbol, int maxDepth, boolean fullQualified, boolean failIfMaxDepthReached) {
        Context context = new Context(maxDepth, fullQualified, failIfMaxDepthReached);
        process(symbol, context);
        return context.formatted();
    }

    @Override
    protected Void visitSymbol(Symbol symbol, Context context) {
        if (context.verifyMaxDepthReached()) {
            return null;
        }
        context.builder.append(symbol.toString());
        return null;
    }

    @Override
    public Void visitAggregation(Aggregation symbol, Context context) {
        if (context.verifyMaxDepthReached()) {
            return null;
        }

        context.builder.append(symbol.functionIdent().name()).append(PAREN_OPEN);
        formatArgs(symbol.inputs(), context);
        context.builder.append(PAREN_CLOSE);
        return null;
    }

    @Override
    public Void visitFunction(Function function, Context context) {
        if (context.verifyMaxDepthReached()) {
            return null;
        }

        FunctionFormatter formatter = null;
        OperatorFormatter operatorFormatter = null;
        if (functions != null) {
            FunctionImplementation<?> impl = functions.get(function.info().ident());
            if (impl instanceof FunctionFormatter) {
                formatter = (FunctionFormatter)impl;
            } else if (impl instanceof OperatorFormatter) {
                operatorFormatter = (OperatorFormatter)impl;
            }
        }
        if (operatorFormatter != null) {
            formatOperator(function, operatorFormatter, context);
        } else {
            if (formatter == null) {
                formatter = SIMPLE_FUNCTION_FORMATTER;
            }
            formatFunction(function, formatter, context);
        }
        return null;
    }

    private void formatOperator(Function function, OperatorFormatter formatter, Context context) {
        int numArgs = function.arguments().size();
        switch (numArgs) {
            case 1:
                context.builder.append(PAREN_OPEN).append(formatter.operator(function)).append(" ");
                context.down();
                function.arguments().get(0).accept(this, context);
                context.up();
                context.builder.append(PAREN_CLOSE);
                break;
            case 2:
                context.builder.append(PAREN_OPEN);
                context.down();
                function.arguments().get(0).accept(this, context);
                context.up();
                context.builder.append(WS)
                               .append(formatter.operator(function))
                               .append(WS);
                context.down();
                function.arguments().get(1).accept(this, context);
                context.up();
                context.builder.append(PAREN_CLOSE);
                break;
            default:
                throw new UnsupportedOperationException("cannot format operators with more than 2 operands");
        }
    }

    private void formatFunction(Function function, FunctionFormatter formatter, Context context) {
        context.builder.append(formatter.beforeArgs(function));
        if (formatter.formatArgs(function)) {
            formatArgs(function.arguments(), context);
        }
        context.builder.append(formatter.afterArgs(function));
    }

    private void formatArgs(List<Symbol> args, Context context) {
        context.down();
        try {
            for (int i = 0, size=args.size(); i < size; i++) {
                args.get(i).accept(this, context);
                if (i < size-1) {
                    context.builder.append(COMMA).append(WS);
                }
            }
        } finally {
            context.up();
        }
    }

    @Override
    public Void visitReference(Reference symbol, Context context) {
        if (context.verifyMaxDepthReached()) {
            return null;
        }

        if (context.isFullQualified()) {
            context.builder.append(symbol.info().ident().tableIdent().sqlFqn())
                           .append(DOT);
        }
        context.builder.append(symbol.info().ident().columnIdent().sqlFqn());
        return null;
    }

    @Override
    public Void visitDynamicReference(DynamicReference symbol, Context context) {
        return visitReference(symbol, context);
    }

    @Override
    public Void visitField(Field field, Context context) {
        if (context.verifyMaxDepthReached()) {
            return null;
        }

        if (context.isFullQualified()) {
            context.builder.append(RelationNamer.INSTANCE.process(field.relation(), null))
                           .append(DOT);
        }
        context.builder.append(field.path().outputName());
        return null;
    }

    @Override
    public Void visitLiteral(Literal symbol, Context context) {
        if (context.verifyMaxDepthReached()) {
            return null;
        }

        LiteralValueFormatter.INSTANCE.format(symbol.value(), context.builder);
        return null;
    }

    /**
     * knows about how to formatSymbol a function, even special ones
     */
    public interface FunctionFormatter {
        /**
         * stuff that comes before arguments are formatted
         */
        String beforeArgs(Function function);

        /**
         * stuff that comes after arguments are formatted
         * @return
         */
        String afterArgs(Function function);

        /**
         * whether or not to formatSymbol the arguments
         */
        boolean formatArgs(Function function);
    }

    public interface OperatorFormatter {
        /**
         * return the operator sign
         */
        String operator(Function function);
    }

    /**
     * fits for the standard case
     */
    private static class SimpleFunctionFormatter implements FunctionFormatter {

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
    }

    public static class RelationNamer extends AnalyzedRelationVisitor<Void, String> {

        public static final RelationNamer INSTANCE = new RelationNamer();

        @Override
        protected String visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return relation.getClass().getCanonicalName();
        }

        @Override
        public String visitTableRelation(TableRelation tableRelation, Void context) {
            return tableRelation.tableInfo().ident().sqlFqn();
        }

        @Override
        public String visitDocTableRelation(DocTableRelation relation, Void context) {
            return relation.tableInfo().ident().sqlFqn();
        }

        @Override
        public String visitQueriedDocTable(QueriedDocTable table, Void context) {
            return table.tableRelation().tableInfo().ident().sqlFqn();
        }

        @Override
        public String visitQueriedTable(QueriedTable table, Void context) {
            return table.tableRelation().tableInfo().ident().sqlFqn();
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
    }
}
