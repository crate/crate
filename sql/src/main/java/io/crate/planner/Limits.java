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

import com.google.common.base.Optional;
import io.crate.analyze.symbol.Symbol;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.arithmetic.AddFunction;

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

    public static Optional<Symbol> add(Optional<Symbol> s1, Optional<Symbol> s2) {
        if (s1.isPresent()) {
            if (s2.isPresent()) {
                return Optional.of((Symbol) AddFunction.of(s1.get(), s2.get()));
            }
            return s1;
        }
        return s2;
    }
}
