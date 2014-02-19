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

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DynamicReference extends Reference {

    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            return new DynamicReference();
        }
    };
                                                                          // DEFAULT TYPE
    private final ReferenceInfo.Builder builder = ReferenceInfo.builder().type(DataType.NULL);

    public DynamicReference() {}

    public DynamicReference(ReferenceInfo info) {
        this.builder.fromInfo(info);
    }

    public DynamicReference(ReferenceIdent ident, RowGranularity rowGranularity) {
        this.builder.ident(ident).granularity(rowGranularity);
    }

    @Override
    public ReferenceInfo info() {
        return builder.build();
    }

    public void valueType(DataType dataType) {
        this.builder.type(dataType);
    }

    public void objectType(ReferenceInfo.ObjectType objectType) {
        this.builder.objectType(objectType);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.DYNAMIC_REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitDynamicReference(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ReferenceInfo info = new ReferenceInfo();
        info.readFrom(in);
        this.builder.fromInfo(info);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        builder.build().writeTo(out);
    }
}
