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

package io.crate.planner.selectivity;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.statistics.Stats;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

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

    public static long estimateNumRows(Stats stats, Symbol query) {
        return (long) (stats.numDocs() * query.accept(SelectivityEstimator.INSTANCE, stats));
    }

    static class SelectivityEstimator extends SymbolVisitor<Stats, Double> {

        static final SelectivityEstimator INSTANCE = new SelectivityEstimator();

        @Override
        protected Double visitSymbol(Symbol symbol, Stats stats) {
            return 1.0;
        }

        @Override
        public Double visitLiteral(Literal literal, Stats stats) {
            Object value = literal.value();
            if (value instanceof Boolean) {
                Boolean val = (Boolean) value;
                return (val == null || !val)
                    ? 0.0
                    : 1.0;
            }
            return super.visitLiteral(literal, stats);
        }

        @Override
        public Double visitFunction(Function function, Stats stats) {
            switch (function.info().ident().name()) {
                case AndOperator.NAME: {
                    double selectivity = 1.0;
                    for (Symbol argument : function.arguments()) {
                        selectivity *= argument.accept(this, stats);
                    }
                    return selectivity;
                }

                case OrOperator.NAME: {
                    double sel1 = 1.0;
                    for (Symbol argument : function.arguments()) {
                        double sel2 = argument.accept(this, stats);
                        sel1 = sel1 + sel2 - sel1 * sel2;
                    }
                    return sel1;
                }

                case EqOperator.NAME: {
                    List<Symbol> arguments = function.arguments();
                    return eqSelectivity(arguments.get(0), arguments.get(1), stats);
                }

                case NotPredicate.NAME: {
                    return 1.0 - function.arguments().get(0).accept(this, stats);
                }

                case IsNullPredicate.NAME: {
                    var arguments = function.arguments();
                    return isNullSelectivity(arguments.get(0), stats);
                }

                default:
                    return MAGIC_SEL;
            }
        }

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

    private static double eqSelectivity(Symbol leftArg, Symbol rightArg, Stats stats) {
        ColumnIdent column = getColumn(leftArg);
        if (column == null) {
            return DEFAULT_EQ_SEL;
        }
        var columnStats = stats.statsByColumn().get(column);
        if (columnStats == null) {
            return DEFAULT_EQ_SEL;
        }
        if (!(rightArg instanceof Literal)) {
            return 1.0 / columnStats.approxDistinct();
        }
        var value = ((Literal<?>) rightArg).value();
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
        if (symbol instanceof Reference) {
            return ((Reference) symbol).column();
        } else if (symbol instanceof Field) {
            return ((Field) symbol).path();
        } else {
            return null;
        }
    }
}
