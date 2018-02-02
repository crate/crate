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

package io.crate.expression.reference.sys.node.local;

import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.expression.NestableInput;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.expression.reference.ReferenceResolver;

import java.util.HashMap;
import java.util.Map;

public class NodeSysReferenceResolver implements ReferenceResolver<NestableInput<?>> {

    private final NodeSysExpression nodeSysExpression;
    private final Map<String, NestableInput> expressionCache;

    public NodeSysReferenceResolver(NodeSysExpression nodeSysExpression) {
        this.nodeSysExpression = nodeSysExpression;
        this.expressionCache = new HashMap<>();
    }

    private NestableInput getCachedImplementation(String name) {
        NestableInput impl = expressionCache.get(name);
        if (impl == null) {
            impl = nodeSysExpression.getChild(name);
            expressionCache.put(name, impl);
        }
        return impl;
    }

    @Override
    public NestableInput getImplementation(Reference refInfo) {
        ReferenceIdent ident = refInfo.ident();
        if (SysNodesTableInfo.SYS_COL_NAME.equals(ident.columnIdent().name())) {
            if (ident.columnIdent().isTopLevel()) {
                return nodeSysExpression;
            }
            NestableInput impl = null;
            for (String part : ident.columnIdent().path()) {
                if (impl == null) {
                    impl = getCachedImplementation(part);
                } else {
                    impl = impl.getChild(part);
                }
            }
            return impl;
        }
        return null;
    }
}

