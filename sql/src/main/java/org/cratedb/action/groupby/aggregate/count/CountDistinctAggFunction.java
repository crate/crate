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

package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Collection;

public class CountDistinctAggFunction extends AggFunction<CountDistinctAggState> {

    public static final String NAME = "COUNT_DISTINCT";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean iterate(CountDistinctAggState state, Object columnValue) {
        if (columnValue != null) {
            // to improve readability in the groupingCollector the seenValues.add is done here
            // if the seenValues is shared across multiple states this means that the add operation
            // is executed multiple times. TODO: move to collector if performance is too bad.
            state.seenValues.add(columnValue);
        }
        return true;
    }

    @Override
    public Collection<DataType> supportedColumnTypes() {
        return DataType.ALL_TYPES;
    }

    @Override
    public boolean supportsDistinct() {
        return true;
    }
}
