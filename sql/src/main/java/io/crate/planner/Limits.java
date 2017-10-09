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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.arithmetic.ArithmeticFunctions;
import io.crate.operation.scalar.conditional.LeastFunction;

import javax.annotation.Nullable;

public class Limits {

    private final int finalLimit;
    private final int offset;

    Limits(int finalLimit, int offset) {
        this.offset = offset;
        this.finalLimit = finalLimit;
    }

    public int finalLimit() {
        return finalLimit;
    }

    public boolean hasLimit() {
        return finalLimit != TopN.NO_LIMIT;
    }

    public int offset() {
        return offset;
    }

    @Nullable
    public static Symbol mergeAdd(@Nullable Symbol s1, @Nullable Symbol s2) {
        if (s1 == null) {
            return s2;
        }
        if (s2 == null) {
            return s1;
        }
        return ArithmeticFunctions.of(
            ArithmeticFunctions.Names.ADD,
            s1,
            s2,
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT
        );
    }

    @Nullable
    public static Symbol mergeMin(@Nullable Symbol limit1, @Nullable Symbol limit2) {
        if (limit1 == null) {
            return limit2;
        }
        if (limit2 == null) {
            return limit1;
        }
        return new Function(LeastFunction.TWO_LONG_INFO, ImmutableList.of(limit1, limit2));
    }
}
