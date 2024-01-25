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

package io.crate.analyze.where;



import java.util.Locale;
import java.util.Set;
import java.util.Stack;
import java.util.function.Supplier;

import io.crate.exceptions.VersioningValidationException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.sql.tree.ComparisonExpression;

public final class WhereClauseValidator {
    private final Visitor visitor;

    public WhereClauseValidator(EvaluatingNormalizer normalizer) {
        this.visitor = new Visitor(normalizer);
    }

    public void validate(Symbol query) {
        query.accept(visitor, new Visitor.Context());
    }

    private static class Visitor extends SymbolVisitor<Visitor.Context, Symbol> {

        static class Context {

            private final Stack<Function> functions = new Stack<>();

            private Context() {
            }
        }

        private Visitor(EvaluatingNormalizer normalizer) {
            this.normalizer = normalizer;
        }

        private static final String SCORE = "_score";
        private static final Set<String> SCORE_ALLOWED_COMPARISONS = Set.of(GteOperator.NAME);

        private static final String VERSION = "_version";
        private static final String SEQ_NO = "_seq_no";
        private static final String PRIMARY_TERM = "_primary_term";
        private static final Set<String> VERSIONING_ALLOWED_COMPARISONS = Set.of(
            EqOperator.NAME, AnyNeqOperator.NAME);

        private static final String SCORE_ERROR = String.format(Locale.ENGLISH,
                                                                "System column '%s' can only be used within a '%s' comparison without any surrounded predicate",
                                                                SCORE, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue());

        private final EvaluatingNormalizer normalizer;

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
            if (function.signature().getKind().equals(FunctionType.TABLE)) {
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
                if (function.name().equals(NotPredicate.NAME)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean insideCastComparedWithLiteral(Context context, Set<String> requiredFunctionNames) {
            var numFunctions = context.functions.size();
            if (numFunctions < 2) {
                return false;
            }
            var lastFunction = context.functions.get(numFunctions - 1);
            var parentFunction = context.functions.get(numFunctions - 2);

            if (lastFunction.isCast()
                && parentFunction.name().startsWith(Operator.PREFIX)
                && requiredFunctionNames.contains(parentFunction.name())) {
                var rightArg = parentFunction.arguments().get(1);
                return rightArg.symbolType().isValueSymbol();
            }
            return false;
        }

        private void validateSysReference(Context context, String columnName) {
            if (columnName.equalsIgnoreCase(VERSION)) {
                validateSysReference(context, VERSIONING_ALLOWED_COMPARISONS, VersioningValidationException::versionInvalidUsage);
            } else if (columnName.equalsIgnoreCase(SEQ_NO) || columnName.equalsIgnoreCase(PRIMARY_TERM)) {
                validateSysReference(context, VERSIONING_ALLOWED_COMPARISONS, VersioningValidationException::seqNoAndPrimaryTermUsage);
            } else if (columnName.equalsIgnoreCase(SCORE)) {
                validateSysReference(context, SCORE_ALLOWED_COMPARISONS, () -> new UnsupportedOperationException(SCORE_ERROR));
            } else if (columnName.equalsIgnoreCase(DocSysColumns.RAW.name())) {
                throw new UnsupportedOperationException("The _raw column is not searchable and cannot be used inside a query");
            }
        }

        private void validateSysReference(Context context, Set<String> requiredFunctionNames, Supplier<RuntimeException> error) {
            if (context.functions.isEmpty()) {
                throw error.get();
            }
            Function function = context.functions.lastElement();
            if ((!insideCastComparedWithLiteral(context, requiredFunctionNames)
                && !requiredFunctionNames.contains(function.name().toLowerCase(Locale.ENGLISH)))
                || insideNotPredicate(context)) {
                throw error.get();
            }
            assert function.arguments().size() == 2 : "function's number of arguments must be 2";
            // normalize the right arg to identify `<expr>` in `_version = <expr>` can be normalized to a literal
            Symbol right = normalizer.normalize(function.arguments().get(1), CoordinatorTxnCtx.systemTransactionContext());
            if (right == null || (!right.symbolType().isValueSymbol() && right.symbolType() != SymbolType.PARAMETER)) {
                throw error.get();
            }
        }
    }
}
