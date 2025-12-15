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

package io.crate.expression.reference.partitioned;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ScopedRef;

public class PartitionExpression implements NestableCollectExpression<PartitionName, Object> {

    private final ScopedRef ref;
    private final int valuesIndex;
    private Object value;

    public PartitionExpression(ScopedRef ref, int valuesIndex) {
        this.ref = ref;
        this.valuesIndex = valuesIndex;
    }

    @Override
    public void setNextRow(PartitionName row) {
        assert row != null : "row shouldn't be null for PartitionExpression";
        value = ref.valueType().implicitCast(row.values().get(valuesIndex));
    }

    @Override
    public Object value() {
        return value;
    }

    public ScopedRef reference() {
        return ref;
    }
}
