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

import java.util.List;

import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;

public final class GroupAndAggregateSemantics {

    /**
     * Ensures that the SELECT list expressions and GROUP BY expressions are semantically valid.
     *
     * The combination of SELECT list and GROUP BY is semantically valid if it can be executed considering the logical execution order of operators:
     * <pre>
     * {@code
     *   (operators that are not relevant are omitted)
     *
     *     Eval (select list)    \
     *      |                    |  these can only operate on top of expressions that are outputs of (Group)HashAggregate
     *     ProjectSet            |
     *      |                    |
     *     WindowAgg            /
     *      |
     *     (Group)HashAggregate  --> Aggregates can also operate on columns that are not part of the select List
     *      |
     *     Collect               --> Can output all columns of the table and eval scalars; no other expressions
     *
     *  To note here:
     *      - Scalars can be evaluated both, within `Collect` *before* an aggregation or grouping, and after in `Eval`
     *      - ProjectSet (table functions) and WindowAgg happen after the Aggregation
     * }
     * </pre>
     */
    public static void validate(List<Symbol> outputSymbols,
                                List<Symbol> groupBy) throws IllegalArgumentException {
        boolean containsAggregations = SymbolVisitors.any(
            x -> x instanceof Function fn && fn.signature().getKind() == FunctionType.AGGREGATE,
            outputSymbols
        );
        if (!containsAggregations && groupBy.isEmpty()) {
            return;
        }
        groupBy.forEach(GroupAndAggregateSemantics::ensureTypedGroupKey);

        for (int i = 0; i < outputSymbols.size(); i++) {
            Symbol output = outputSymbols.get(i);
            Symbol offender = output.accept(FindOffendingSymbol.INSTANCE, groupBy);
            if (offender == null) {
                continue;
            }
            throw new IllegalArgumentException(
                "'" + offender +
                "' must appear in the GROUP BY clause or be used in an aggregation function. " +
                "Perhaps you grouped by an alias that clashes with a column in the relations"
            );
        }
    }

    private static void ensureTypedGroupKey(Symbol groupBy) {
        groupBy.accept(EnsureTypedGroupKey.INSTANCE, null);
    }

    private static class EnsureTypedGroupKey extends SymbolVisitor<Void, Void> {

        private static final EnsureTypedGroupKey INSTANCE = new EnsureTypedGroupKey();

        @Override
        public Void visitLiteral(Literal<?> symbol, Void context) {
            if (symbol.valueType() == DataTypes.UNDEFINED) {
                if (symbol.value() == null) {
                    // `NULL` is a valid case
                    return null;
                } else {
                    raiseException(symbol);
                }
            }
            return null;
        }

        @Override
        public Void visitAlias(AliasSymbol aliasSymbol, Void context) {
            return aliasSymbol.symbol().accept(this, context);
        }

        private static void raiseException(Symbol symbol) {
            throw new IllegalArgumentException(
                "Cannot group or aggregate on '" + symbol.toString() + "' with an undefined type." +
                " Using an explicit type cast will make this work but adds processing overhead to the query."
            );
        }
    }

    private static class FindOffendingSymbol extends SymbolVisitor<List<Symbol>, Symbol> {

        private static final FindOffendingSymbol INSTANCE = new FindOffendingSymbol();

        @Override
        protected Symbol visitSymbol(Symbol symbol, List<Symbol> groupBy) {
            throw new UnsupportedOperationException("Unsupported symbol: " + symbol);
        }

        @Override
        public Symbol visitFunction(Function function, List<Symbol> groupBy) {
            switch (function.signature().getKind()) {
                case SCALAR: {
                    /* valid:
                     *  SELECT 4 * x FROM tbl GROUP BY x
                     *  SELECT 4 * x FROM tbl GROUP BY 4 * x
                     *
                     * invalid:
                     *  SELECT 4 * y FROM tbl GROUP BY x
                     */
                    if (groupBy.contains(function)) {
                        return null;
                    }
                    for (Symbol argument : function.arguments()) {
                        Symbol offender = argument.accept(this, groupBy);
                        if (offender != null) {
                            return function;
                        }
                    }
                    return null;
                }

                case AGGREGATE:
                    return null;

                case TABLE:
                case WINDOW:
                    // Cannot group by a table or window function. Arguments must pass validation:
                    for (Symbol argument : function.arguments()) {
                        Symbol offender = argument.accept(this, groupBy);
                        if (offender != null) {
                            return offender;
                        }
                    }
                    return null;

                default:
                    throw new IllegalStateException("Unexpected function type: " + function.signature().getKind());
            }
        }

        @Override
        public Symbol visitAggregation(Aggregation symbol, List<Symbol> groupBy) {
            throw new AssertionError("`Aggregation` symbols are created in the Planner. Until then there should only be `Function` symbols with type aggregate");
        }

        @Override
        public Symbol visitAlias(AliasSymbol aliasSymbol, List<Symbol> groupBy) {
            /* valid:
             *      SELECT x AS xx, count(*) FROM tbl GROUP BY xx;
             *      SELECT x AS xx, count(*) FROM tbl GROUP BY x;
             *
             * not valid:
             *
             *      SELECT x AS xx, count(*) FROM tbl GROUP BY y;
             */
            if (groupBy.contains(aliasSymbol)) {
                return null;
            }
            return aliasSymbol.symbol().accept(this, groupBy);
        }

        @Override
        public Symbol visitReference(Reference ref, List<Symbol> groupBy) {
            if (containedIn(ref, groupBy)) {
                return null;
            }
            return ref;
        }

        @Override
        public Symbol visitField(ScopedSymbol symbol, List<Symbol> groupBy) {
            if (containedIn(symbol, groupBy)) {
                return null;
            }
            return symbol;
        }

        public static boolean containedIn(Symbol symbol, List<Symbol> groupBy) {
            // SELECT count(*), x AS xx, x FROM tbl GROUP BY 2
            // GROUP BY is on `xx`, but `x` is implicitly also present in GROUP BY, so must be valid.
            for (Symbol groupExpr : groupBy) {
                if (symbol.equals(groupExpr)) {
                    return true;
                }
                if (groupExpr instanceof AliasSymbol aliasSymbol) {
                    if (symbol.equals(aliasSymbol.symbol())) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Symbol visitDynamicReference(DynamicReference ref, List<Symbol> groupBy) {
            return visitReference(ref, groupBy);
        }

        @Override
        public Symbol visitWindowFunction(WindowFunction function, List<Symbol> groupBy) {
            // Window function is executed after the GROUP operation
            // It cannot appear in the GROUP BY clause, but arguments must pass validation.
            for (Symbol argument : function.arguments()) {
                Symbol offender = argument.accept(this, groupBy);
                if (offender != null) {
                    return offender;
                }
            }
            return null;
        }

        @Override
        public Symbol visitLiteral(Literal<?> symbol, List<Symbol> groupBy) {
            return null;
        }

        @Override
        public Symbol visitParameterSymbol(ParameterSymbol parameterSymbol, List<Symbol> groupBy) {
            // Behaves like a literal: `SELECT $1, x GROUP BY x` is allowed
            return null;
        }

        @Override
        public Symbol visitSelectSymbol(SelectSymbol selectSymbol, List<Symbol> groupBy) {
            // Only non-correlated sub-queries are allowed, so this behaves like a literal
            return null;
        }

        @Override
        public Symbol visitInputColumn(InputColumn inputColumn, List<Symbol> groupBy) {
            throw new AssertionError("Must not have `InputColumn`s when doing semantic validation of SELECT LIST / GROUP BY");
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, List<Symbol> groupBy) {
            throw new AssertionError("MATCH predicate cannot be used in SELECT list");
        }

        @Override
        public Symbol visitFetchReference(FetchReference fetchReference, List<Symbol> groupBy) {
            throw new AssertionError("Must not have `FetchReference`s when doing semantic validation of SELECT LIST / GROUP BY");
        }
    }
}

