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

package io.crate.planner.operators;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.JoinType;

/**
 * Helper to detect if a join is an Equi join and could be executed with a hash join algorithm.
 * <p>
 * Using hash join is possible under following assumptions:
 * <ul>
 * <li>it's a {@link JoinType#INNER} join type</li>
 * <li>the join condition contains no {@link OrOperator}</li>
 * <li>the join condition contains at least one {@link EqOperator}</li>
 * <li>at least one argument of the {@link EqOperator} must NOT contain fields to multiple tables</li>
 * </ul>
 */
public class EquiJoinDetector {

    private static final Visitor VISITOR = new Visitor();

    public static boolean isHashJoinPossible(JoinType joinType, Symbol joinCondition) {
        if (joinType != JoinType.INNER) {
            return false;
        }
        return isEquiJoin(joinCondition);
    }

    public static boolean isEquiJoin(Symbol joinCondition) {
        assert joinCondition != null : "join condition must not be null on inner joins";
        Context context = new Context();
        joinCondition.accept(VISITOR, context);
        return context.isHashJoinPossible;
    }

    /**
     * insideEqualOperand is used to mark starting point of the computation of the either sides of equality.
     *
     */
    private static class Context {
        boolean exit = false;
        boolean isHashJoinPossible = false;
        boolean insideEqualOperand = false;
        Set<RelationName> relations = new HashSet<>();
    }

    private static class Visitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function function, Context context) {
            if (context.exit) {
                return null;
            }
            String functionName = function.name();
            switch (functionName) {
                case NotPredicate.NAME -> {
                    if (context.insideEqualOperand) {
                        // Not a top-level expression but inside EQ operator.
                        // We need to collect all relations.
                        for (Symbol arg : function.arguments()) {
                            arg.accept(this, context);
                        }
                    } else {
                        return null;
                    }
                }
                case OrOperator.NAME -> {
                    context.isHashJoinPossible = false;
                    context.exit = true;
                    return null;
                }
                case EqOperator.NAME -> {
                    List<Symbol> arguments = function.arguments();
                    var left = arguments.get(0);
                    var right = arguments.get(1);
                    if (context.insideEqualOperand) {
                        // EQ operator inside either side of the top level EQ operator
                        // We need to re-use the same context to ensure that we keep counting all relations of each JOIN side.
                        left.accept(this, context);
                        right.accept(this, context);
                    } else {
                        // Top level EQ operator of the JOIN condition.
                        // There can be nested EQ operators but this is top level equal.
                        // Start collecting relations for both sides.
                        var leftContext = new Context();
                        leftContext.insideEqualOperand = true;
                        left.accept(this, leftContext);

                        var rightContext = new Context();
                        rightContext.insideEqualOperand = true;
                        right.accept(this, rightContext);

                        if (leftContext.relations.size() == 1 &&
                            rightContext.relations.size() == 1 &&
                            !leftContext.relations.equals(rightContext.relations)) {
                            context.isHashJoinPossible = true;
                        }
                    }
                }
                default -> {
                    for (Symbol arg : function.arguments()) {
                        arg.accept(this, context);
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitField(ScopedSymbol field, Context context) {
            context.relations.add(field.relation());
            return null;
        }

        @Override
        public Void visitReference(Reference ref, Context context) {
            context.relations.add(ref.ident().tableIdent());
            return null;
        }
    }
}
