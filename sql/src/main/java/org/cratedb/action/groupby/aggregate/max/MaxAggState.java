/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action.groupby.aggregate.max;

import org.cratedb.action.groupby.aggregate.AggState;

public abstract class MaxAggState<T extends Comparable<T>> extends AggState<MaxAggState<T>> {

    private T value = null;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(MaxAggState<T> other) {
        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }

        if (compareTo(other) < 0) {
            value = other.value;
        }
    }


    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public int compareTo(MaxAggState<T> o) {
        if (o == null) return -1;
        return compareValue(o.value);
    }

    public int compareValue(T otherValue) {
        if (value == null) return (otherValue == null ? 0 : -1);
        if (otherValue == null) return 1;

        return value.compareTo(otherValue);
    }
}
