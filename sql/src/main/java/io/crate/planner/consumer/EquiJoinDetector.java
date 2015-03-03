/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import org.elasticsearch.common.collect.IdentityHashSet;

import javax.annotation.Nullable;
import java.util.Set;


/**
 * a query is considered an equi join criteria
 * if the query consists a group of = operators chained by AND
 *
 * Examples:
 *  * t1.b = t2.c --> equi join
 *  * not t1.b = t2.c AND t1.b = t2.a --> no equi join
 *  * t1.a = t2.b AND t1.c = t2.d --> equi join
 *  * t1.a in (t2.b, t2.c, t2.d) --> t1.a = t2.b OR t1.a = t2.c OR ... --> no equi join
 *
 */
public class EquiJoinDetector {

    private static final EquiJoinVisitor VISITOR = new EquiJoinVisitor();

    public static boolean isEquiJoinCondition(Symbol joinCondition) {
        return VISITOR.isEquiJoin(joinCondition);
    }

    private static class EquiJoinVisitor extends SymbolVisitor<EquiJoinVisitor.Context, Boolean> {

        public static class Context {
            IdentityHashSet<AnalyzedRelation> referencedRelations = new IdentityHashSet<>();
        }

        private static final Set<String> ALLOWED_FUNCTIONS = ImmutableSet.of(
                EqOperator.NAME, AndOperator.NAME
        );

        public boolean isEquiJoin(@Nullable Symbol query) {
            if (query == null) {
                return false;
            }
            Context ctx = new Context();
            Boolean result = process(query, ctx);
            return result && ctx.referencedRelations.size() > 1;
        }

        @Override
        public Boolean visitFunction(Function symbol, Context context) {
            if (!ALLOWED_FUNCTIONS.contains(symbol.info().ident().name())) {
                return Boolean.FALSE;
            }
            for (Symbol arg : symbol.arguments()) {
                if (!process(arg, context)) {
                    return Boolean.FALSE;
                }
            }
            return Boolean.TRUE;
        }

        @Override
        public Boolean visitField(Field field, Context context) {
            context.referencedRelations.add(field.relation());
            return Boolean.TRUE;
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Context context) {
            return Boolean.TRUE;
        }
    }


}
