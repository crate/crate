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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.List;

import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;

public class RewriteToSourceLookupWithoutAggregates implements Rule<Order> {

    private Pattern<Order> pattern;
    private Capture<Collect> collectCapture;
    private volatile boolean enabled = true;

    public RewriteToSourceLookupWithoutAggregates() {
        this.collectCapture = new Capture<>();
        this.pattern = typeOf(Order.class)
            .with(source(), typeOf(Collect.class).capturedAs(collectCapture));
    }

    @Override
    public Pattern<Order> pattern() {
        return this.pattern;
    }

    @Override
    public boolean isEnabled() {
        return this.enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public LogicalPlan apply(Order order,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             Functions functions) {
        Collect collect = captures.get(collectCapture);
        if (collect.preferSourceLookup()) {
            return null;
        }
        // Predicate<Symbol> notUsedInOrderBy = x -> {
        //     return !order.orderBy().orderBySymbols().contains(x);
        // };
        // List<Symbol> newOutputs = Lists2.map(collect.outputs(), x -> {
        //     return notUsedInOrderBy.test(x)
        //         ? DocReferences.toSourceLookup(x)
        //         : x;
        // });
        // if (newOutputs.equals(collect.outputs())) {
        //     return null;
        // }
        return order.replaceSources(List.of(new Collect(
            true,
            collect.relation(),
            collect.outputs(),
            collect.where(),
            collect.numExpectedRows(),
            collect.estimatedRowSize()
        )));
    }
}
