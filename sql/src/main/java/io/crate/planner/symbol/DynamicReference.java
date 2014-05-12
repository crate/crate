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
import io.crate.types.DataType;
import io.crate.types.NullType;
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

    private ReferenceInfo info;

    public DynamicReference() {}

    public DynamicReference(ReferenceInfo info) {
        this.info = info;
    }

    public DynamicReference(ReferenceIdent ident, RowGranularity rowGranularity) {
        this.info = new ReferenceInfo(ident, rowGranularity, NullType.INSTANCE);
    }

    @Override
    public ReferenceInfo info() {
        return info;
    }

    public void valueType(DataType dataType) {
        assert this.info != null;
        this.info = new ReferenceInfo(info.ident(), info.granularity(), dataType, info.objectType());
    }

    public void objectType(ReferenceInfo.ObjectType objectType) {
        assert this.info != null;
        this.info = new ReferenceInfo(info.ident(), info.granularity(), info.type(), objectType);
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
        info = new ReferenceInfo();
        info.readFrom(in);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
    }
}
