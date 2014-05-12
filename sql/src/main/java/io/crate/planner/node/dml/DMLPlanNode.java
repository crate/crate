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

package io.crate.planner.node.dml;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import io.crate.types.LongType;

import java.util.List;
import java.util.Set;

public abstract class DMLPlanNode implements DQLPlanNode {

    // output just contains the affectedRows in most cases (at least until the RETURNING clause is supported)
    private static final List<DataType> OUTPUT_TYPES = ImmutableList.<DataType>of(LongType.INSTANCE);

    protected List<DataType> inputTypes = ImmutableList.of();

    @Override
    public boolean hasProjections() {
        return false;
    }

    @Override
    public List<Projection> projections() {
        return ImmutableList.of();
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    @Override
    public List<DataType> inputTypes() {
        return inputTypes;
    }

    @Override
    public void inputTypes(List<DataType> dataTypes) {
        inputTypes = dataTypes;
    }

    @Override
    public List<DataType> outputTypes() {
        return OUTPUT_TYPES;
    }

    @Override
    public void outputTypes(List<DataType> outputTypes) {
        throw new UnsupportedOperationException("outputTypes cannot be modified on DMLPlanNodes");
    }
}
