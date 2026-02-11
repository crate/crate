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


import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

import io.crate.exceptions.VersioningValidationException;
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
import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;
import io.crate.sql.tree.ComparisonExpression;

public final class WhereClauseValidator {

    private static final Visitor VISITOR = new Visitor();

    private WhereClauseValidator() {}

    public static void validate(Symbol query) {
        query.accept(VISITOR, new ArrayDeque<>());
    }

    private static class Visitor extends SymbolVisitor<ArrayDeque<Function>, Symbol> {

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

        @Override
        public Symbol visitField(ScopedSymbol field, ArrayDeque<Function> functions) {
            validateSysReference(functions, field.column().sqlFqn());
            return super.visitField(field, functions);
        }

        @Override
        public Symbol visitReference(Reference symbol, ArrayDeque<Function> functions) {
            validateSysReference(functions, symbol.column().name());
            return super.visitReference(symbol, functions);
        }

        @Override
        public Symbol visitFunction(Function function, ArrayDeque<Function> functions) {
            functions.push(function);
            if (function.signature().getType().equals(FunctionType.TABLE)) {
                throw new UnsupportedOperationException("Table functions are not allowed in WHERE");
            }
            for (Symbol argument : function.arguments()) {
                argument.accept(this, functions);
            }
            functions.pop();
            return function;
        }

        @Override
        public Symbol visitWindowFunction(WindowFunction symbol, ArrayDeque<Function> functions) {
            throw new IllegalArgumentException("Window functions are not allowed in WHERE");
        }

        private static boolean insideNotPredicate(ArrayDeque<Function> functions) {
            for (Function function : functions) {
                if (function.name().equals(NotPredicate.NAME)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean insideCastComparedWithLiteral(ArrayDeque<Function> functions, Set<String> requiredFunctionNames) {
            var numFunctions = functions.size();
            if (numFunctions < 2) {
                return false;
            }
            var lastFunction = functions.pollFirst();
            var parentFunction = functions.peekFirst();
            functions.push(lastFunction);

            if (lastFunction.isCast()
                && parentFunction.name().startsWith(Operator.PREFIX)
                && requiredFunctionNames.contains(parentFunction.name())) {
                var rightArg = parentFunction.arguments().get(1);
                return rightArg.symbolType().isValueSymbol();
            }
            return false;
        }

        private void validateSysReference(ArrayDeque<Function> functions, String columnName) {
            if (columnName.equalsIgnoreCase(VERSION)) {
                validateSysReference(functions, VERSIONING_ALLOWED_COMPARISONS, VersioningValidationException::versionInvalidUsage);
            } else if (columnName.equalsIgnoreCase(SEQ_NO) || columnName.equalsIgnoreCase(PRIMARY_TERM)) {
                validateSysReference(functions, VERSIONING_ALLOWED_COMPARISONS, VersioningValidationException::seqNoAndPrimaryTermUsage);
            } else if (columnName.equalsIgnoreCase(SCORE)) {
                validateSysReference(functions, SCORE_ALLOWED_COMPARISONS, () -> new UnsupportedOperationException(SCORE_ERROR));
            } else if (columnName.equalsIgnoreCase(SysColumns.RAW.name())) {
                throw new UnsupportedOperationException("The _raw column is not searchable and cannot be used inside a query");
            }
        }

        private static void validateSysReference(ArrayDeque<Function> functions, Set<String> requiredFunctionNames, Supplier<RuntimeException> error) {
            if (functions.isEmpty()) {
                throw error.get();
            }
            Function function = functions.peekFirst();
            if ((!insideCastComparedWithLiteral(functions, requiredFunctionNames)
                && !requiredFunctionNames.contains(function.name().toLowerCase(Locale.ENGLISH)))
                || insideNotPredicate(functions)) {
                throw error.get();
            }
            if (function.arguments().size() > 1) {
                Symbol right = function.arguments().get(1);
                if (!right.symbolType().isValueSymbol() && right.symbolType() != SymbolType.PARAMETER) {
                    throw error.get();
                }
            }
        }
    }
}
