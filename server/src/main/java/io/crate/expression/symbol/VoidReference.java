/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.symbol;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;

/**
 * A reference to a column which does not exist. This is primarily used for object columns where lookups like `some_column['nested_column']` are allowed if the session `error_on_unknown_object_key` setting is set to false.
 **/
public class VoidReference extends DynamicReference {

    public VoidReference(StreamInput in) throws IOException {
        super(in);
    }

    public VoidReference(ReferenceIdent ident,
                         int position) {
        super(ident, RowGranularity.DOC, position);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.VOID_REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitVoidReference(this, context);
    }
}
