/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.node;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.SimpleObjectExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.reference.NestedObjectExpression;
import org.elasticsearch.common.inject.Inject;

public class NodeSysExpression extends NestedObjectExpression {

    private final SysNodesTableInfo tableInfo;
    private final ReferenceResolver resolver;

    @Inject
    public NodeSysExpression(SysNodesTableInfo tableInfo, ReferenceResolver resolver) {
        this.tableInfo = tableInfo;
        this.resolver = resolver;
        for (ReferenceInfo info : tableInfo.columns()) {
            childImplementations.put(
                    info.ident().columnIdent().name(),
                    resolver.getImplementation(info.ident()));
        }
    }

    public ReferenceImplementation getImplementation(ReferenceIdent ident) {
        if (!tableInfo.tableColumn().name().equals(ident.columnIdent().name())) {
            return null;
        }
        if (ident.columnIdent().isColumn()) {
            return this;
        } else {
            return resolver.getImplementation(tableInfo.tableColumn().getImplementationIdent(ident));
        }
    }
}
