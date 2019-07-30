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

package io.crate.expression.symbol;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.ExplainLeaf;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Aggregation extends Symbol {

    public Aggregation(StreamInput in) throws IOException {
        functionInfo = new FunctionInfo(in);
        valueType = DataTypes.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_4_1_0)) {
            filter = Symbols.fromStream(in);
        } else {
            filter = Literal.BOOLEAN_TRUE;
        }
        inputs = Symbols.listFromStream(in);
    }

    private final FunctionInfo functionInfo;
    private final List<Symbol> inputs;
    private final DataType valueType;
    private final Symbol filter;

    public Aggregation(FunctionInfo functionInfo, DataType valueType, List<Symbol> inputs) {
        this(functionInfo, valueType, inputs, Literal.BOOLEAN_TRUE);
    }

    public Aggregation(FunctionInfo functionInfo,
                       DataType valueType,
                       List<Symbol> inputs,
                       Symbol filter) {
        Preconditions.checkNotNull(inputs, "inputs must not be null");
        Preconditions.checkNotNull(filter, "filter must not be null");

        this.valueType = valueType;
        this.functionInfo = functionInfo;
        this.inputs = inputs;
        this.filter = filter;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.AGGREGATION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public DataType valueType() {
        return valueType;
    }

    public FunctionIdent functionIdent() {
        return functionInfo.ident();
    }

    public List<Symbol> inputs() {
        return inputs;
    }

    public Symbol filter() {
        return filter;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        functionInfo.writeTo(out);
        DataTypes.toStream(valueType, out);
        if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
            Symbols.toStream(filter, out);
        }
        Symbols.toStream(inputs, out);

    }

    @Override
    public String toString() {
        return representation();
    }

    @Override
    public String representation() {
        return "Aggregation{" + functionInfo.ident().name() +
               ", args=[" + ExplainLeaf.printList(inputs) + "]" +
               ", filter=" + filter.representation() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Aggregation that = (Aggregation) o;
        return Objects.equals(functionInfo, that.functionInfo) &&
               Objects.equals(inputs, that.inputs) &&
               Objects.equals(valueType, that.valueType) &&
               Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionInfo, inputs, valueType, filter);
    }
}
