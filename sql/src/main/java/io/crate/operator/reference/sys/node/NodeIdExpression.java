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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operator.reference.sys.node;

import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SystemReferences;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.Discovery;


public class NodeIdExpression extends SysExpression<BytesRef> {

    public static final String COLNAME = "id";

    public static final ReferenceInfo INFO_ID = SystemReferences.registerNodeReference(
            COLNAME, DataType.STRING);

    private final BytesRef value;

    @Inject
    public NodeIdExpression(Discovery discovery) {
        this.value = new BytesRef(discovery.localNode().getId());
    }

    @Override
    public BytesRef value() {
        return value;
    }

    @Override
    public ReferenceInfo info() {
        return INFO_ID;
    }

}
