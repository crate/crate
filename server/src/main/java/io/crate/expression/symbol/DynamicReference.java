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

package io.crate.expression.symbol;

import io.crate.metadata.SimpleReference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DynamicReference extends SimpleReference {

    private final ColumnPolicy columnPolicy;

    public DynamicReference(ReferenceIdent ident, RowGranularity granularity, int position, ColumnPolicy columnPolicy) {
        super(ident, granularity, DataTypes.UNDEFINED, position, null);
        this.columnPolicy = columnPolicy;
    }

    public DynamicReference(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_5_5_0)) {
            columnPolicy = in.readEnum(ColumnPolicy.class);
        } else {
            columnPolicy = ColumnPolicy.DYNAMIC;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_5_0)) {
            out.writeEnum(columnPolicy);
        }
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.DYNAMIC_REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitDynamicReference(this, context);
    }

    public void valueType(DataType<?> targetType) {
        type = targetType;
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DynamicReference that = (DynamicReference) o;
        return columnPolicy == that.columnPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnPolicy);
    }
}
