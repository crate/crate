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

import io.crate.types.DataTypes;
import io.crate.types.Regclass;

import javax.annotation.Nullable;

public final class RegclassSizeEstimator extends SizeEstimator<Regclass> {

    public static final RegclassSizeEstimator INSTANCE = new RegclassSizeEstimator();

    private final SizeEstimator<Integer> oidEstimator;

    public RegclassSizeEstimator() {
        this.oidEstimator = SizeEstimatorFactory.create(DataTypes.INTEGER);
    }

    @Override
    public long estimateSize(@Nullable Regclass value) {
        if (value == null) {
            return 8;
        }
        return StringSizeEstimator.estimate(value.name()) + oidEstimator.estimateSize(value.oid());
    }
}
