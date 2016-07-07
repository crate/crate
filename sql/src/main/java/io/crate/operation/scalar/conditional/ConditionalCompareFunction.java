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

package io.crate.operation.scalar.conditional;

import com.google.common.collect.Ordering;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

abstract class ConditionalCompareFunction extends ConditionalFunction {
    protected final Ordering<Object> ordering;

    ConditionalCompareFunction(FunctionInfo info) {
        super(info);
        this.ordering = Ordering.from(new InputComparator());
    }

    @Override
    public Object evaluate(Input... args) {
        List<Object> values = new ArrayList<>();
        for (Input input : args) {
            values.add(input.value());
        }
        return compare(values);
    }

    protected abstract Object compare(List<Object> values);

    private class InputComparator implements Comparator {
        @Override
        public int compare(Object o1, Object o2) {
            return info().returnType().compareValueTo(o1, o2);
        }
    }
}
