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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.arithmetic.ArithmeticFunctions;
import io.crate.operation.scalar.conditional.LeastFunction;

import java.util.Arrays;
import java.util.Optional;

public class Limits {

    private final int finalLimit;
    private final int limitAndOffset;
    private final int offset;

    Limits(int finalLimit, int offset) {
        this.offset = offset;
        this.finalLimit = finalLimit;
        if (finalLimit > TopN.NO_LIMIT) {
            this.limitAndOffset = finalLimit + offset;
        } else {
            this.limitAndOffset = TopN.NO_LIMIT;
        }
    }

    public int finalLimit() {
        return finalLimit;
    }

    public int limitAndOffset() {
        return limitAndOffset;
    }

    public boolean hasLimit() {
        return finalLimit != TopN.NO_LIMIT;
    }

    public int offset() {
        return offset;
    }

    public static Optional<Symbol> mergeAdd(Optional<Symbol> s1, Optional<Symbol> s2) {
        if (s1.isPresent()) {
            if (s2.isPresent()) {
                return Optional.of(ArithmeticFunctions.of(
                    ArithmeticFunctions.Names.ADD,
                    s1.get(),
                    s2.get(),
                    FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT
                ));
            }
            return s1;
        }
        return s2;
    }

    public static Optional<Symbol> mergeMin(Optional<Symbol> limit1, Optional<Symbol> limit2) {
        if (limit1.isPresent()) {
            if (limit2.isPresent()) {
                return Optional.of(new Function(LeastFunction.TWO_LONG_INFO,
                    Arrays.asList(limit1.get(), limit2.get())));
            }
            return limit1;
        }
        return limit2;
    }
}
