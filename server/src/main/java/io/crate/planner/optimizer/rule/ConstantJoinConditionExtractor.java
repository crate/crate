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

import java.util.LinkedHashMap;
import java.util.SequencedMap;
import java.util.Set;

import io.crate.analyze.RelationNames;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;

public class ConstantJoinConditionExtractor {

    public static SequencedMap<Set<RelationName>, Symbol> apply(Symbol symbol) {
        var allConditions = QuerySplitter.split(symbol);
        var constantConditions = new LinkedHashMap<Set<RelationName>, Symbol>();
        for (var condition : allConditions.entrySet()) {
            if (numberOfRelationsUsed(condition.getValue()) <= 1) {
                constantConditions.put(condition.getKey(), condition.getValue());
            }
        }
        return constantConditions;
    }

    private static int numberOfRelationsUsed(Symbol joinCondition) {
        return RelationNames.getShallow(joinCondition).size();
    }
}
