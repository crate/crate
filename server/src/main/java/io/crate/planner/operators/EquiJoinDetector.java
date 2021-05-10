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

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.join.JoinType;

import java.util.HashSet;
import java.util.Set;

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

    private static boolean isEquiJoin(Symbol joinCondition) {
        assert joinCondition != null : "join condition must not be null on inner joins";
        Context context = new Context();
        joinCondition.accept(VISITOR, context);
        return context.isHashJoinPossible;
    }

    private static class Context {
        boolean isHashJoinPossible = false;
        boolean insideEqOperator = false;
        Set<RelationName> usedRelationsInsideEqOperatorArgument = new HashSet<>();
    }

    private static class Visitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function function, Context context) {
            String functionName = function.name();
            switch (functionName) {
                case AndOperator.NAME:
                    for (Symbol arg : function.arguments()) {
                        arg.accept(this, context);
                    }
                    break;
                case EqOperator.NAME:
                    context.isHashJoinPossible = true;
                    context.insideEqOperator = true;
                    for (Symbol arg : function.arguments()) {
                        arg.accept(this, context);
                        if (context.usedRelationsInsideEqOperatorArgument.size() != 1) {
                            context.isHashJoinPossible = false;
                        }
                        context.usedRelationsInsideEqOperatorArgument = new HashSet<>();
                    }
                    break;
                default:
                    if (context.insideEqOperator) {
                        for (Symbol arg : function.arguments()) {
                            arg.accept(this, context);
                        }
                    } else {
                        context.isHashJoinPossible = false;
                        return null;
                    }
                    break;
            }
            return null;
        }

        @Override
        public Void visitField(ScopedSymbol field, Context context) {
            if (context.insideEqOperator) {
                context.usedRelationsInsideEqOperatorArgument.add(field.relation());
            }
            return null;
        }

        @Override
        public Void visitReference(Reference ref, Context context) {
            if (context.insideEqOperator) {
                context.usedRelationsInsideEqOperatorArgument.add(ref.ident().tableIdent());
            }
            return null;
        }
    }
}
