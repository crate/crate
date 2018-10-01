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

package io.crate.execution.dsl.projection.builder;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionInfo;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;


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

    private final List<Symbol> toCollect;
    private final List<Symbol> standalone;
    private final List<Function> aggregatesOrTableFunctions;

    @Nullable
    private final FunctionInfo.Type functionsType;

    SplitPoints(List<Symbol> toCollect,
                List<Symbol> standalone,
                List<Function> aggregatesOrTableFunctions,
                @Nullable FunctionInfo.Type functionsType) {
        this.toCollect = toCollect;
        this.standalone = standalone;
        this.aggregatesOrTableFunctions = aggregatesOrTableFunctions;
        this.functionsType = functionsType;
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public List<Function> aggregates() {
        if (FunctionInfo.Type.AGGREGATE.equals(functionsType)) {
            return aggregatesOrTableFunctions;
        } else {
            return Collections.emptyList();
        }
    }

    public List<Function> tableFunctions() {
        if (FunctionInfo.Type.TABLE.equals(functionsType)) {
            return aggregatesOrTableFunctions;
        } else {
            return Collections.emptyList();
        }
    }

    public List<Symbol> standalone() {
        return standalone;
    }
}
