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

package io.crate.analyze.where;


import com.google.common.collect.ImmutableSet;
import io.crate.exceptions.VersioninigValidationException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.sql.tree.ComparisonExpression;

import java.util.Locale;
import java.util.Set;
import java.util.Stack;
import java.util.function.Supplier;

public final class WhereClauseValidator {

    private static final Visitor VISITOR = new Visitor();

    private WhereClauseValidator() {
    }

    public static void validate(Symbol query) {
        query.accept(VISITOR, new Visitor.Context());
    }

    private static class Visitor extends SymbolVisitor<Visitor.Context, Symbol> {

        static class Context {

            private final Stack<Function> functions = new Stack<>();

            private Context() {
            }
        }

        private static final String SCORE = "_score";
        private static final Set<String> SCORE_ALLOWED_COMPARISONS = ImmutableSet.of(GteOperator.NAME);

        private static final String VERSION = "_version";
        private static final String SEQ_NO = "_seq_no";
        private static final String PRIMARY_TERM = "_primary_term";
        private static final Set<String> VERSIONING_ALLOWED_COMPARISONS = ImmutableSet.of(
            EqOperator.NAME, AnyOperators.Names.NEQ);

        private static final String SCORE_ERROR = String.format(Locale.ENGLISH,
                                                                "System column '%s' can only be used within a '%s' comparison without any surrounded predicate",
                                                                SCORE, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue());

        @Override
        public Symbol visitField(ScopedSymbol field, Context context) {
            validateSysReference(context, field.column().sqlFqn());
            return super.visitField(field, context);
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            validateSysReference(context, symbol.column().name());
            return super.visitReference(symbol, context);
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            context.functions.push(function);
            if (function.info().type().equals(FunctionInfo.Type.TABLE)) {
                throw new UnsupportedOperationException("Table functions are not allowed in WHERE");
            }
            continueTraversal(function, context);
            context.functions.pop();
            return function;
        }

        @Override
        public Symbol visitWindowFunction(WindowFunction symbol, Context context) {
            throw new IllegalArgumentException("Window functions are not allowed in WHERE");
        }

        private void continueTraversal(Function symbol, Context context) {
            for (Symbol argument : symbol.arguments()) {
                argument.accept(this, context);
            }
        }

        private static boolean insideNotPredicate(Context context) {
            for (Function function : context.functions) {
                if (function.info().ident().name().equals(NotPredicate.NAME)) {
                    return true;
                }
            }
            return false;
        }

        private void validateSysReference(Context context, String columnName) {
            if (columnName.equalsIgnoreCase(VERSION)) {
                validateSysReference(context, VERSIONING_ALLOWED_COMPARISONS, VersioninigValidationException::versionInvalidUsage);
            } else if (columnName.equalsIgnoreCase(SEQ_NO) || columnName.equalsIgnoreCase(PRIMARY_TERM)) {
                validateSysReference(context, VERSIONING_ALLOWED_COMPARISONS, VersioninigValidationException::seqNoAndPrimaryTermUsage);
            } else if (columnName.equalsIgnoreCase(SCORE)) {
                validateSysReference(context, SCORE_ALLOWED_COMPARISONS, () -> new UnsupportedOperationException(SCORE_ERROR));
            } else if (columnName.equalsIgnoreCase(DocSysColumns.RAW.name())) {
                throw new UnsupportedOperationException("The _raw column is not searchable and cannot be used inside a query");
            }
        }

        private static void validateSysReference(Context context, Set<String> requiredFunctionNames, Supplier<RuntimeException> error) {
            if (context.functions.isEmpty()) {
                throw error.get();
            }
            Function function = context.functions.lastElement();
            if (!requiredFunctionNames.contains(function.info().ident().name().toLowerCase(Locale.ENGLISH))
                || insideNotPredicate(context)) {
                throw error.get();
            }
            assert function.arguments().size() == 2 : "function's number of arguments must be 2";
            Symbol right = function.arguments().get(1);
            if (!right.symbolType().isValueSymbol()) {
                throw error.get();
            }
        }
    }
}
