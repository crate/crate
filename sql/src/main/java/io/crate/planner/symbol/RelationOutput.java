/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.symbol;

import com.google.common.base.MoreObjects;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RelationOutput extends Symbol {

    public static final SymbolFactory<RelationOutput> FACTORY = new SymbolFactory<RelationOutput>() {
        @Override
        public RelationOutput newInstance() {
            return new RelationOutput();
        }
    };

    public static Symbol unwrap(Symbol symbol) {
        return UNWRAPPING_VISITOR.process(symbol, null);
    }

    private static final UnwrappingVisitor UNWRAPPING_VISITOR = new UnwrappingVisitor();
    private AnalyzedRelation relation;
    private Symbol target;

    public RelationOutput(AnalyzedRelation relation, Symbol target) {
        this.relation = relation;
        this.target = target;
    }

    private RelationOutput() {}

    public AnalyzedRelation relation() {
        return relation;
    }

    public Symbol target() {
        return target;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.RELATION_OUTPUT;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitRelationOutput(this, context);
    }

    @Override
    public DataType valueType() {
        return target.valueType();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("RelationOutput is not streamable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RelationOutput is not streamable");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(RelationOutput.class).add("target", target).add("relation", relation).toString();
    }

    private static class UnwrappingVisitor extends SymbolVisitor<Void, Symbol>
    {
        @Override
        public Symbol visitRelationOutput(RelationOutput relationOutput, Void context) {
            return process(relationOutput.target(), context);
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Void context) {
            return symbol;
        }
    }
}
