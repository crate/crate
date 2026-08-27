/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.LinkedHashMap;
import java.util.Map;

import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Fetch;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.MapBackedSymbolReplacer;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public final class MergeEvalIntoFetch implements Rule<Eval> {

    private final Pattern<Eval> pattern = typeOf(Eval.class).with(source(), typeOf(Fetch.class));

    @Override
    public Pattern<Eval> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Eval eval, Captures captures, Rule.Context context) {
        Fetch fetch = (Fetch) eval.source();
        Map<Symbol, Symbol> fetchReplacedOutputs = fetch.replacedOutputs();
        LinkedHashMap<Symbol, Symbol> newReplacedOutputs = new LinkedHashMap<>();
        // Eval's source is Fetch, so each Eval output is a part of Fetch outputs,
        // it just some extra computation on top if it.
        // Hence, it's safe to lookup each Eval output in Fetch's replacedOutputs.
        for (Symbol output : eval.outputs()) {
            newReplacedOutputs.put(output, MapBackedSymbolReplacer.convert(output, fetchReplacedOutputs));
        }
        return new Fetch(
            newReplacedOutputs,
            fetch.fetchRefs(),
            fetch.fetchSourceByRelation(),
            fetch.sources().get(0)
        );
    }
}
