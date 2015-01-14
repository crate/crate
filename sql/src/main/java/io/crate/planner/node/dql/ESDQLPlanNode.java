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

package io.crate.planner.node.dql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;

import java.util.List;
import java.util.Set;

public abstract class ESDQLPlanNode implements DQLPlanNode, PlannedAnalyzedRelation {

    protected List<Symbol> outputs;
    private List<DataType> inputTypes;
    private List<DataType> outputTypes;

    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public boolean hasProjections() {
        return false;
    }

    @Override
    public List<Projection> projections() {
        return ImmutableList.of();
    }

    @Override
    public void inputTypes(List<DataType> dataTypes) {
        this.inputTypes = dataTypes;
    }

    @Override
    public List<DataType> inputTypes() {
        return inputTypes;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    @Override
    public void outputTypes(List<DataType> outputTypes) {
        this.outputTypes = outputTypes;
    }

    @Override
    public List<DataType> outputTypes() {
        return outputTypes;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitPlanedAnalyzedRelation(this, context);
    }

    @javax.annotation.Nullable
    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField is not supported on ESDQLPlanNode");
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getWritableField is not supported on ESDQLPlanNode");
    }

    @Override
    public List<Field> fields() {
        throw new UnsupportedOperationException("fields is not supported on ESDQLPlanNode");
    }
}
