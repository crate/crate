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

package io.crate.expression.symbol;

import java.util.List;
import java.util.Objects;

public class WindowFunctionContext {

    private final WindowFunction function;
    private final List<Symbol> inputs;
    private final Symbol filter;

    public WindowFunctionContext(WindowFunction function,
                                 List<Symbol> inputs,
                                 Symbol filter) {
        this.function = function;
        this.inputs = inputs;
        this.filter = filter;
    }

    public WindowFunction function() {
        return function;
    }

    public List<Symbol> inputs() {
        return inputs;
    }

    public Symbol filter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowFunctionContext that = (WindowFunctionContext) o;
        return Objects.equals(function, that.function) &&
               Objects.equals(inputs, that.inputs) &&
               Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, inputs, filter);
    }
}
