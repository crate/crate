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

package org.cratedb.action.groupby.aggregate.any;

import org.cratedb.action.groupby.aggregate.AggState;

public abstract class AnyAggState<T> extends AggState<AnyAggState<T>>{

    public T value = null;

    @Override
    public Object value() {
        return value;
    }

    public abstract void add(Object otherValue);

    @Override
    public void reduce(AnyAggState<T> other) {
        this.value = other.value;
    }

    @Override
    public int compareTo(AnyAggState<T> o) {
        if (o == null) return 1;
        if (value == null) return (o.value == null ? 0 : -1);
        if (o.value == null) return 1;

        return 0; // any two object that are not null are considered equal
    }
}
