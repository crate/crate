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

package io.crate.planner.node;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.CopyAnalysis;
import org.cratedb.DataType;

import java.util.List;
import java.util.Set;

public class CopyNode extends PlanNode {

    private static final List<DataType> OUTPUT_TYPES = ImmutableList.of(DataType.LONG);

    private final CopyAnalysis.Mode mode;
    private final String path;
    private final String index;

    public CopyNode(String path, String index, CopyAnalysis.Mode mode) {
        this.path = path;
        this.index = index;
        this.mode = mode;
    }

    public String path() {
        return path;
    }

    public String index() {
        return index;
    }

    public CopyAnalysis.Mode mode() {
        return mode;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCopyNode(this, context);
    }

    @Override
    public List<DataType> outputTypes() {
        return OUTPUT_TYPES;
    }
}
