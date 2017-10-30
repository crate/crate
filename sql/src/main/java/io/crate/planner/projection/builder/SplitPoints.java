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

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;

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

    public static SplitPoints create(QueriedRelation relation) {
        SplitPoints splitPoints = new SplitPoints();
        if (relation.hasAggregates() || !relation.groupBy().isEmpty()) {
            SplitPointVisitor.addAggregatesAndToCollectSymbols(relation, splitPoints);
        } else {
            OrderBy orderBy = relation.orderBy();
            if (orderBy == null) {
                splitPoints.toCollect.addAll(relation.outputs());
            } else {
                splitPoints.toCollect.addAll(Lists2.concatUnique(relation.outputs(), orderBy.orderBySymbols()));
            }
        }
        return splitPoints;
    }

    private SplitPoints() {
        this.toCollect = new ArrayList<>();
        this.aggregates = new ArrayList<>();
    }

    public ArrayList<Symbol> toCollect() {
        return toCollect;
    }

    public ArrayList<Function> aggregates() {
        return aggregates;
    }
}
