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

package io.crate.planner.projection.builder;

import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;

import java.util.ArrayList;


/**
 * SplitPoints contain a separated representation of aggregations and toCollect (aggregation sources).
 * They can be created from a QuerySpec which contains aggregations.
 * <pre>
 *     Example:
 *
 *     Input QuerySpec:
 *       outputs: [add(sum(coalesce(x, 10)), 10)]
 *
 *     SplitPoints:
 *       aggregations:  [sum(coalesce(x, 10))]
 *       toCollect:     [coalesce(x, 10)]
 * </pre>
 */
public class SplitPoints {

    private final ArrayList<Symbol> toCollect;
    private final ArrayList<Function> aggregates;

    public static SplitPoints create(QuerySpec querySpec) {
        SplitPoints splitPoints = new SplitPoints(querySpec);
        SplitPointVisitor.addAggregatesAndToCollectSymbols(querySpec, splitPoints);
        return splitPoints;
    }

    private SplitPoints(QuerySpec querySpec) {
        int estimate = querySpec.outputs().size();
        this.toCollect = new ArrayList<>(estimate);
        this.aggregates = new ArrayList<>(estimate);
    }

    public ArrayList<Symbol> toCollect() {
        return toCollect;
    }

    public ArrayList<Function> aggregates() {
        return aggregates;
    }
}
