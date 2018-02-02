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

package io.crate.planner.operators;

import io.crate.analyze.relations.JoinPair;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.planner.node.dql.join.JoinType;

/**
 * Helper to detect if a join could be executed with a hash join algorithm.
 * <p>
 * Using hash join is possible under following assumptions:
 * <ul>
 * <li>it's a {@link JoinType#INNER} join type</li>
 * <li>the join condition contains at least one {@link EqOperator}</li>
 * <li>the join condition contains no {@link OrOperator}</li>
 * </ul>
 */
public class HashJoinDetector {

    private static final Visitor VISITOR = new Visitor();

    public static boolean isHashJoinPossible(JoinPair joinPair) {
        if (joinPair.joinType() != JoinType.INNER) {
            return false;
        }
        Symbol joinCondition = joinPair.condition();
        assert joinCondition != null : "join condition must not be null on inner joins";
        Context context = new Context();
        VISITOR.process(joinPair.condition(), context);
        return context.isHashJoinPossible;
    }

    private static class Context {
        boolean isHashJoinPossible = false;
    }

    private static class Visitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function function, Context context) {
            String functionName = function.info().ident().name();
            if (functionName.equals(OrOperator.NAME)) {
                context.isHashJoinPossible = false;
                return null;
            } else if (functionName.equals(EqOperator.NAME)) {
                context.isHashJoinPossible = true;
            }
            for (Symbol arg : function.arguments()) {
                process(arg, context);
            }
            return null;
        }
    }
}
