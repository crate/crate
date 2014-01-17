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

package io.crate.analyze;

import io.crate.metadata.*;
import io.crate.metadata.sys.SystemReferences;
import io.crate.sql.tree.*;
import org.cratedb.DataType;

import java.util.IdentityHashMap;
import java.util.Map;

public class BindingContext {

    private final ReferenceResolver referenceResolver;
    private final Functions functions;
    private TableIdent table;
    private Map<FunctionCall, FunctionInfo> functionInfos = new IdentityHashMap<>();
    private Map<Expression, ReferenceInfo> referenceInfos = new IdentityHashMap<>();

    public BindingContext(ReferenceResolver referenceResolver, Functions functions) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
    }

    public void table(TableIdent table) {
        this.table = table;
    }

    public TableIdent table() {
        return this.table;
    }

    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceResolver.getInfo(ident);
        if (info == null) {
            throw new UnsupportedOperationException("TODO: unknown column reference?");
        }
        return info;
    }

    public FunctionInfo getFunctionInfo(FunctionIdent ident) {
        FunctionImplementation implementation = functions.get(ident);
        if (implementation == null) {
            throw new UnsupportedOperationException("TODO: unknown function? " + ident.toString());
        }
        return implementation.info();
    }

    public void putFunctionInfo(FunctionCall node, FunctionInfo functionInfo) {
        functionInfos.put(node, functionInfo);
    }

    public void putReferenceInfo(Expression node, ReferenceInfo info) {
        referenceInfos.put(node, info);
    }

    public ReferenceInfo getReferenceInfo(SubscriptExpression node) {
        return referenceInfos.get(node);
    }

    public FunctionInfo getFunctionInfo(FunctionCall node) {
        return functionInfos.get(node);
    }
}
