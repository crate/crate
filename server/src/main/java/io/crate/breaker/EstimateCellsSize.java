/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.breaker;

import java.util.Collection;
import java.util.List;
import java.util.function.ToLongFunction;

import io.crate.common.collections.Lists2;
import io.crate.types.DataType;

public final class EstimateCellsSize implements ToLongFunction<Object[]> {

    private final List<SizeEstimator<Object>> estimators;

    public EstimateCellsSize(Collection<? extends DataType<?>> columnTypes) {
        this.estimators = Lists2.map(columnTypes, SizeEstimatorFactory::create);
    }

    @Override
    public long applyAsLong(Object[] cells) {
        assert estimators.size() == cells.length
            : "Size of incoming cells must match number of estimators. "
                + "Cells=" + cells.length
                + " estimators=" + estimators.size();
        long size = 0;
        for (int i = 0; i < cells.length; i++) {
            size += estimators.get(i).estimateSize(cells[i]);
        }
        return size;
    }
}
