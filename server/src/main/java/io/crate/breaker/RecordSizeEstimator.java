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

import javax.annotation.Nullable;

import io.crate.data.Row;

import java.util.List;

public final class RecordSizeEstimator extends SizeEstimator<Row> {

    private final List<SizeEstimator<? super Object>> fieldEstimators;

    public RecordSizeEstimator(List<SizeEstimator<? super Object>> fieldEstimators) {
        this.fieldEstimators = fieldEstimators;
    }

    @Override
    public long estimateSize(@Nullable Row value) {
        if (value == null) {
            return 8;
        }
        assert fieldEstimators.size() == value.numColumns()
            : "The row must have the same number of fields as `fieldEstimators` are available";

        long size = 0;
        for (int i = 0; i < value.numColumns(); i++) {
            size += fieldEstimators.get(i).estimateSize(value.get(i));
        }
        return size;
    }
}
