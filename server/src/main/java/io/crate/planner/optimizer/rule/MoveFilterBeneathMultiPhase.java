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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.Util.transpose;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.MultiPhase;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class MoveFilterBeneathMultiPhase implements Rule<Filter> {

    private Pattern<Filter> pattern;
    private Capture<MultiPhase> multiPhaseCapture;

    public MoveFilterBeneathMultiPhase() {
        this.multiPhaseCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(MultiPhase.class).capturedAs(multiPhaseCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter, Captures captures, Context context) {
        MultiPhase multiPhase = captures.get(multiPhaseCapture);
        ArrayList<Symbol> toPushDown = new ArrayList<>();
        ArrayList<Symbol> toKeep = new ArrayList<>();
        Symbol predicate = filter.query();
        List<Symbol> queryParts = AndOperator.split(predicate);
        Predicate<Symbol> isSubQueryPart =
            x -> x instanceof SelectSymbol
            && multiPhase.subQueries().values().contains(x);
        for (var part : queryParts) {
            if (part.any(isSubQueryPart)) {
                toKeep.add(part);
            } else {
                toPushDown.add(part);
            }
        }
        if (toPushDown.isEmpty()) {
            return null;
        }
        if (toKeep.isEmpty()) {
            return transpose(filter, multiPhase);
        }
        Filter pushedDownFilter = new Filter(multiPhase.source(), AndOperator.join(toPushDown));
        return new Filter(
            multiPhase.replaceSources(List.of(pushedDownFilter)),
            AndOperator.join(toKeep)
        );
    }
}
