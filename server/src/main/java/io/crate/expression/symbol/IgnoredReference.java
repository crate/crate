/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.symbol;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;

/**
 * A reference to a sub-column of an ignored object that cannot be found from {@link DocTableInfo}.
 * <p>
 * ex) create table c1 (o object(ignored) as (a int));
 *     insert into c1 values ({b=1});
 *     select o['a'], o['b'], o['c'] from c1;
 *     ==> o['a'] is a SimpleReference
 *         o['b'], o['c'] are IgnoredReferences
 * <p>
 * The difference between {@link VoidReference} is that 1) this is for IGNORED objects, 2) the columns may exist and contain actual values.
 */
public class IgnoredReference extends DynamicReference {
    public IgnoredReference(ReferenceIdent ident,
                            RowGranularity granularity,
                            int position) {
        super(ident, granularity, position);
    }

    public IgnoredReference(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.IGNORED_REFERENCE;
    }
}
