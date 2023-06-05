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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.statistics.Stats;

public final class PrintContext {

    private final StringBuilder sb;
    private final ArrayList<String> prefixes = new ArrayList<>();
    @Nullable
    private final PlanStats planStats;

    public PrintContext(@Nullable PlanStats planStats) {
        this.planStats = planStats;
        sb = new StringBuilder();
    }

    public PrintContext text(String s) {
        sb.append(s);
        return this;
    }

    @SafeVarargs
    public final PrintContext nest(Consumer<PrintContext>... children) {
        return nest(Arrays.asList(children));
    }

    public final PrintContext nest(List<Consumer<PrintContext>> children) {
        for (int i = 0; i < children.size(); i++) {
            sb.append("\n");
            for (String prefix : prefixes) {
                sb.append(prefix);
            }
            if (i + 1 == children.size()) {
                sb.append("  └ ");
                prefixes.add("  ");
            } else {
                sb.append("  ├ ");
                prefixes.add("  │");
            }
            Consumer<PrintContext> child = children.get(i);
            child.accept(this);
            prefixes.remove(prefixes.size() - 1);
        }
        return this;
    }

    String stats(LogicalPlan logicalPlan) {
        if (planStats == null) {
            return "";
        }
        var stats = planStats.get(logicalPlan);
        if (stats == Stats.EMPTY) {
            return "";
        }
        return " (rows: " + stats.numDocs() + ", width: " + stats.sizeInBytes() + ")";
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}
