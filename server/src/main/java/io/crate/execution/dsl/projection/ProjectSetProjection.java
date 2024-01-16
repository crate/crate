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

package io.crate.execution.dsl.projection;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.Lists;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;

public final class ProjectSetProjection extends Projection {

    private final List<Symbol> tableFunctionsWithInputs;
    private final List<Symbol> standaloneWithInputs;
    private final List<Symbol> outputs;

    public ProjectSetProjection(List<Symbol> tableFunctionsWithInputs, List<Symbol> standaloneWithInputs) {
        assert tableFunctionsWithInputs.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + tableFunctionsWithInputs;
        assert standaloneWithInputs.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + standaloneWithInputs;
        this.tableFunctionsWithInputs = tableFunctionsWithInputs;
        this.standaloneWithInputs = standaloneWithInputs;
        this.outputs = Lists.concat(tableFunctionsWithInputs, standaloneWithInputs);
    }

    public ProjectSetProjection(StreamInput in) throws IOException {
        tableFunctionsWithInputs = Symbols.listFromStream(in);
        standaloneWithInputs = Symbols.listFromStream(in);
        outputs = Lists.concat(tableFunctionsWithInputs, standaloneWithInputs);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.PROJECT_SET;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitProjectSet(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(tableFunctionsWithInputs, out);
        Symbols.toStream(standaloneWithInputs, out);
    }

    public List<Symbol> tableFunctions() {
        return tableFunctionsWithInputs;
    }

    public List<Symbol> standalone() {
        return standaloneWithInputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProjectSetProjection that = (ProjectSetProjection) o;

        if (!tableFunctionsWithInputs.equals(that.tableFunctionsWithInputs)) return false;
        return standaloneWithInputs.equals(that.standaloneWithInputs);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + tableFunctionsWithInputs.hashCode();
        result = 31 * result + standaloneWithInputs.hashCode();
        return result;
    }
}
