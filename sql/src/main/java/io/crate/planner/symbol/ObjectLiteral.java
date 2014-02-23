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

import com.google.common.base.Joiner;
import org.cratedb.DataType;
import org.cratedb.core.collections.MapComparator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

public class ObjectLiteral extends Literal<Map<String, Object>, ObjectLiteral> {

    public static final SymbolFactory<ObjectLiteral> FACTORY = new SymbolFactory<ObjectLiteral>() {
        @Override
        public ObjectLiteral newInstance() {
            return new ObjectLiteral();
        }
    };

    private Map<String, Object> value;

    ObjectLiteral() {}

    public ObjectLiteral(Map<String, Object> value) {
        this.value = value;
    }

    @Override
    public int compareTo(ObjectLiteral o) {
        return MapComparator.compareMaps(value, o.value);
    }

    @Override
    public Map<String, Object> value() {
        return value;
    }

    @Override
    public DataType valueType() {
        return DataType.OBJECT;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.OBJECT_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitObjectLiteral(this, context);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = (Map<String, Object>) in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
    }

    /**
     * ObjectLiteral cannot be converted to other types
     */
    @Override
    public Literal convertTo(DataType type) {
        return super.convertTo(type);
    }

    @Override
    public String humanReadableName() {
        return String.format("{%s}",
                Joiner.on(", ").withKeyValueSeparator(": ").join(value.entrySet()));
    }
}
