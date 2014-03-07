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

package io.crate.planner.symbol;

import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;

public class Parameter extends Symbol {

    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            return new Parameter();
        }
    };

    private Object value;

    public Parameter() {}

    public Parameter(Object value) {
        this.value = value;
    }

    public Object value() {
        return value;
    }


    @Override
    public SymbolType symbolType() {
        return SymbolType.PARAMETER;
    }

    /**
     * returns the guessed {@link DataType} of the <code>Parameter</code> value.
     * e.g. for to determine if the parameter fits a given type
     */
    @Nullable
    public DataType guessedValueType() {
        return DataType.forValue(value);
    }

    public Literal toLiteral() {
        return Literal.forValue(value);
    }

    public Literal toLiteral(DataType dataType) {
        return Literal.forValue(value).convertTo(dataType);
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitParameter(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
    }
}
