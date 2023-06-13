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

package io.crate.planner.selectivity;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Row;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operators;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.statistics.Stats;

/**
 * Used to estimate the number of rows returned after applying a given query.
 *
 * The numbers, heuristic and logic here is heavily inspired by PostgreSQL.
 * See `src/backend/optimizer/path/clausesel.c` `clause_selectivity`
 */
public class SelectivityFunctions {

    private static final double DEFAULT_EQ_SEL = 0.005;
    /**
     * For all cases where we don't have a concrete selectivity logic we use this magic number.
     * It seems to have worked for PostgreSQL quite well so far.
     */
    private static final double MAGIC_SEL = 0.333;

    public static long estimateNumRows(NodeContext nodeCtx,
                                       TransactionContext txnCtx,
                                       Stats stats,
                                       Symbol query,
                                       @Nullable Row params) {
        var estimator = new SelectivityEstimator(nodeCtx, txnCtx, stats, params);
        Double selectivity = query.accept(estimator, null);
        return (long) (stats.numDocs() * selectivity);
    }

    static class SelectivityEstimator extends SymbolVisitor<Void, Double> {

        private final Stats stats;
        @Nullable
        private final Row params;
        private final NodeContext nodeCtx;
        private final TransactionContext txnCtx;

        SelectivityEstimator(NodeContext nodeCtx, TransactionContext txnCtx, Stats stats, @Nullable Row params) {
            this.nodeCtx = nodeCtx;
            this.txnCtx = txnCtx;
            this.stats = stats;
            this.params = params;
        }


        @Override
        protected Double visitSymbol(Symbol symbol, Void context) {
            return 1.0;
        }

        @Override
        public Double visitLiteral(Literal<?> literal, Void context) {
            Object value = literal.value();
            if (value instanceof Boolean bool) {
                return !bool ? 0.0 : 1.0;
            }
            if (value == null) {
                return 0.0;
            }
            return super.visitLiteral(literal, context);
        }

        @Override
        public Double visitFunction(Function function, Void context) {
            switch (function.name()) {
                case AndOperator.NAME: {
                    double selectivity = 1.0;
                    for (Symbol argument : function.arguments()) {
                        selectivity *= argument.accept(this, context);
                    }
                    return selectivity;
                }

                case OrOperator.NAME: {
                    double sel1 = 1.0;
                    for (Symbol argument : function.arguments()) {
                        double sel2 = argument.accept(this, context);
                        sel1 = sel1 + sel2 - sel1 * sel2;
                    }
                    return sel1;
                }

                case EqOperator.NAME: {
                    List<Symbol> arguments = function.arguments();
                    return eqSelectivity(arguments.get(0), arguments.get(1), stats, params);
                }

                case NotPredicate.NAME: {
                    return 1.0 - function.arguments().get(0).accept(this, context);
                }

                case IsNullPredicate.NAME: {
                    var arguments = function.arguments();
                    return isNullSelectivity(arguments.get(0), stats);
                }

                default:
                    if (Operators.COMPARISON_OPERATORS.contains(function.name())) {
                        return genericOpSelectivity(nodeCtx, txnCtx, stats, function, params);
                    }
                    return MAGIC_SEL;
            }
        }

    }

    @SuppressWarnings("unchecked")
    private static double genericOpSelectivity(NodeContext nodeCtx,
                                               TransactionContext txnCtx,
                                               Stats stats,
                                               Function function,
                                               Row params) {
        assert Operators.COMPARISON_OPERATORS.contains(function.name())
            : "genericOpSelectivity only works for Operators like >, <, <= and >=";


        double defaultSel = MAGIC_SEL;

        List<Symbol> arguments = function.arguments();
        assert arguments.size() == 2 : "Operator must have two arguments";

        Symbol lhs = arguments.get(0);
        ColumnIdent lhsColumn = getColumn(lhs);
        if (lhsColumn == null) {
            return defaultSel;
        }

        Symbol rhs = arguments.get(1);
        final Object rhsValue;
        if (rhs instanceof ParameterSymbol param && params != null) {
            rhsValue = params.get(param.index());
        } else if (rhs instanceof Literal<?> literal) {
            rhsValue = literal.value();
        } else {
            return defaultSel;
        }

        if (rhsValue == null) {
            return 0.0;
        }

        ColumnStats<?> lhsStats = stats.getColumnStats(lhsColumn);
        if (lhsStats == null || lhsStats.mostCommonValues().isEmpty()) {
            return defaultSel;
        }

        Scalar<Boolean, Object> operator = (Scalar<Boolean, Object>) nodeCtx.functions().getQualified(function);
        MostCommonValues mostCommonValues = lhsStats.mostCommonValues();
        double selectivity = 0.0;
        for (int i = 0; i < mostCommonValues.length(); i++) {
            Object value = mostCommonValues.value(i);
            Boolean result = operator.evaluate(txnCtx, nodeCtx, () -> value, () -> rhsValue);
            if (result != null && result) {
                selectivity += mostCommonValues.frequency(i);
            }
        }
        return selectivity;
    }

    private static double isNullSelectivity(Symbol arg, Stats stats) {
        ColumnIdent column = getColumn(arg);
        if (column == null) {
            return MAGIC_SEL;
        }
        var columnStats = stats.statsByColumn().get(column);
        if (columnStats == null) {
            return MAGIC_SEL;
        }
        return columnStats.nullFraction();
    }

    private static double eqSelectivity(Symbol leftArg,
                                        Symbol rightArg,
                                        Stats stats,
                                        @Nullable Row params) {
        ColumnIdent lhsColumn = getColumn(leftArg);
        if (lhsColumn == null) {
            return DEFAULT_EQ_SEL;
        }
        var lhsStats = stats.statsByColumn().get(lhsColumn);
        if (lhsStats == null) {
            return DEFAULT_EQ_SEL;
        }
        if (rightArg instanceof ParameterSymbol param && params != null) {
            var value = params.get(param.index());
            return eqSelectivityFromValueAndStats(value, lhsStats);
        }
        if (rightArg instanceof Literal<?> literal) {
            return eqSelectivityFromValueAndStats(literal.value(), lhsStats);
        }

        if (rightArg instanceof Reference || rightArg instanceof ScopedSymbol) {
            ColumnIdent rhsColumn = getColumn(rightArg);
            if (rhsColumn == null) {
                return 1.0 / lhsStats.approxDistinct();
            }
            var rhsStats = stats.statsByColumn().get(rhsColumn);
            if (rhsStats == null) {
                return 1.0 / lhsStats.approxDistinct();
            }

            MostCommonValues lhsMcv = lhsStats.mostCommonValues();
            MostCommonValues rhsMcv = rhsStats.mostCommonValues();

            if (!lhsMcv.isEmpty() && !rhsMcv.isEmpty()) {
                return selectivityFromMvcMatches(lhsStats, rhsStats);
            }

            double nullfrac1 = lhsStats.nullFraction();
            double nullfrac2 = rhsStats.nullFraction();

            double selectivity = (1.0 - nullfrac1) * (1.0 - nullfrac2);
            if (lhsStats.approxDistinct() > rhsStats.approxDistinct()) {
                return selectivity / lhsStats.approxDistinct();
            } else {
                return selectivity / rhsStats.approxDistinct();
            }
        }

        return 1.0 / lhsStats.approxDistinct();
    }

    private static double clamp(double value) {
        return Math.min(Math.max(value, 0.0), 1.0);
    }

    /**
     * See PostgreSQL src/backend/utils/adt/selfuncs.c  `eqjoinsel_inner`
     */
    private static double selectivityFromMvcMatches(ColumnStats<?> lhsStats, ColumnStats<?> rhsStats) {
        MostCommonValues lhsMcv = lhsStats.mostCommonValues();
        MostCommonValues rhsMcv = rhsStats.mostCommonValues();
        boolean[] hasmatch1 = new boolean[lhsMcv.length()];
        boolean[] hasmatch2 = new boolean[rhsMcv.length()];

        double matchfreq = 0.0;
        int numMatches = 0;
        for (int i = 0; i < lhsMcv.length(); i++) {
            Object lhsValue = lhsMcv.value(i);

            for (int j = 0; j < rhsMcv.length(); j++) {
                if (hasmatch2[j]) {
                    continue;
                }
                Object rhsValue = rhsMcv.value(j);

                if (lhsValue != null && Objects.equals(lhsValue, rhsValue)) {
                    hasmatch1[i] = true;
                    hasmatch2[j] = true;
                    matchfreq += lhsMcv.frequency(i) * rhsMcv.frequency(j);
                    numMatches++;
                    break;
                }
            }
        }
        matchfreq = clamp(matchfreq);

        double matchfreq1 = 0.0;
        double unmatchfreq1 = 0.0;
        for (int i = 0; i < lhsMcv.length(); i++) {
            if (hasmatch1[i]) {
                matchfreq1 += lhsMcv.frequency(i);
            } else {
                unmatchfreq1 += lhsMcv.frequency(i);
            }
        }
        matchfreq1 = clamp(matchfreq1);
        unmatchfreq1 = clamp(unmatchfreq1);

        double matchfreq2 = 0.0;
        double unmatchfreq2 = 0.0;
        for (int i = 0; i < rhsMcv.length(); i++) {
            if (hasmatch2[i]) {
                matchfreq2 = rhsMcv.frequency(i);
            } else {
                unmatchfreq1 = rhsMcv.frequency(i);
            }
        }
        matchfreq2 = clamp(matchfreq2);
        unmatchfreq2 = clamp(unmatchfreq2);

        double otherfreq1 = clamp(1.0 - lhsStats.nullFraction() - matchfreq1 - unmatchfreq1);
        double otherfreq2 = clamp(1.0 - rhsStats.nullFraction() - matchfreq2 - unmatchfreq2);

        double totalsel1 = matchfreq;
        double nd1 = lhsStats.approxDistinct();
        double nd2 = rhsStats.approxDistinct();
        if (nd2 > rhsMcv.length()) {
            totalsel1 += unmatchfreq1 * otherfreq2 / (nd2 - rhsMcv.length());
        }
        if (nd2 > numMatches) {
            totalsel1 += otherfreq1 * (otherfreq2 + unmatchfreq2) / (nd2 - numMatches);
        }

        double totalsel2 = matchfreq;
        if (nd1 > lhsMcv.length()) {
            totalsel2 += unmatchfreq2 * otherfreq1 / (nd1 - lhsMcv.length());
        }
        if (nd1 > numMatches) {
            totalsel2 += otherfreq2 * (otherfreq1 + unmatchfreq1) / (nd1 - numMatches);
        }

        return (totalsel1 < totalsel2) ? totalsel1 : totalsel2;
    }

    private static double eqSelectivityFromValueAndStats(Object value, ColumnStats<?> columnStats) {
        if (value == null) {
            // x = null -> is always false
            return 0.0;
        }
        var mcv = columnStats.mostCommonValues();
        int idx = Arrays.asList(mcv.values()).indexOf(value);
        if (idx < 0) {
            return 1.0 / columnStats.approxDistinct();
        } else {
            return mcv.frequencies()[idx];
        }
    }

    @Nullable
    private static ColumnIdent getColumn(Symbol symbol) {
        if (symbol instanceof Reference ref) {
            return ref.column();
        } else if (symbol instanceof ScopedSymbol) {
            return ((ScopedSymbol) symbol).column();
        } else {
            return null;
        }
    }
}
