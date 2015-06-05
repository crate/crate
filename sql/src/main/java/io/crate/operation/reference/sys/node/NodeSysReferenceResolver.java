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
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;

import java.util.HashMap;
import java.util.Map;

public class NodeSysReferenceResolver implements ReferenceResolver {

    private NodeSysExpression nodeSysExpression;
    private final Map<String, ReferenceImplementation> expressionCache = new HashMap();

    public NodeSysReferenceResolver(NodeSysExpression nodeSysExpression) {
        this.nodeSysExpression = nodeSysExpression;
    }

    private ReferenceImplementation getCachedImplementation(String name) {
        ReferenceImplementation impl = expressionCache.get(name);
        if (impl == null) {
            impl = nodeSysExpression.getChildImplementation(name);
            expressionCache.put(name, impl);
        }
        return impl;
    }

    @Override
    public ReferenceImplementation getImplementation(ReferenceIdent ident) {
        if (SysNodesTableInfo.IDENT.equals(ident.tableIdent())) {
            ReferenceImplementation impl = getCachedImplementation(ident.columnReferenceIdent().columnIdent().name());
            if (impl != null) {
                for (String part : ident.columnIdent().path()) {
                    impl = impl.getChildImplementation(part);
                }
            }
            return impl;
        } else if (SysNodesTableInfo.SYS_COL_NAME.equals(ident.columnIdent().name())) {
            if (ident.columnIdent().isColumn()) {
                return nodeSysExpression;
            }
            ReferenceImplementation impl = null;
            for (String part : ident.columnIdent().path()) {
                if (impl == null) {
                    impl = getCachedImplementation(part);
                } else {
                    impl = impl.getChildImplementation(part);
                }
            }
            return impl;
        }
        return null;
    }
}

