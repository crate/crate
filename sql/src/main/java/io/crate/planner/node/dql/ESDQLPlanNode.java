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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbolVisitor;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public abstract class ESDQLPlanNode implements DQLPlanNode, PlannedAnalyzedRelation {

    private static final char COMMA = ',';

    protected List<Symbol> outputs;
    private List<DataType> inputTypes;
    private List<DataType> outputTypes;

    public List<Symbol> outputs() {
        return outputs;
    }

    @Nullable
    public static String noCommaStringRouting(Optional<Set<Symbol>> clusteredBy) {
        if (clusteredBy.isPresent()){
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Symbol symbol : clusteredBy.get()) {
                String s = ValueSymbolVisitor.STRING.process(symbol);
                if (s.indexOf(COMMA)>-1){
                    return null;
                }
                if (!first){
                    sb.append(COMMA);
                } else {
                    first = false;
                }
                sb.append(s);
            }
            return sb.toString();
        }
        return null;
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
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
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

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addProjection not supported on ESDQLPlanNode");
    }

    @Override
    public boolean resultIsDistributed() {
        throw new UnsupportedOperationException("resultIsDistributed is not supported on ESDQLPlanNode");
    }

    @Override
    public DQLPlanNode resultNode() {
        throw new UnsupportedOperationException("resultNode is not supported on ESDQLPLanNode");
    }

    @Override
    public Plan plan() {
        return new IterablePlan(this);
    }
}
