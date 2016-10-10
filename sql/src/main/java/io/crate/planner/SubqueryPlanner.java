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

package io.crate.planner;

import com.google.common.base.Function;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SubqueryPlanner implements Function<Symbol, Symbol> {

    private final Planner.Context plannerContext;
    private final Visitor visitor;
    private final List<Plan> subQueries = new ArrayList<>();

    public SubqueryPlanner(Planner.Context plannerContext) {
        this.plannerContext = plannerContext;
        this.visitor = new Visitor();
    }

    private Symbol planSubquery(SelectSymbol selectSymbol) {
        AnalyzedRelation relation = selectSymbol.relation();
        SelectAnalyzedStatement selectAnalyzedStatement = new SelectAnalyzedStatement(((QueriedRelation) relation));
        Plan subPlan = plannerContext.plan(selectAnalyzedStatement);
        subQueries.add(subPlan);
        return selectSymbol;
    }

    @Nullable
    @Override
    public Symbol apply(@Nullable Symbol input) {
        return visitor.process(input, null);
    }

    public List<Plan> subQueries() {
        return subQueries;
    }

    private class Visitor extends ReplacingSymbolVisitor<Void> {

        public Visitor() {
            super(ReplaceMode.MUTATE);
        }

        @Override
        public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            return planSubquery(selectSymbol);
        }
    }
}
