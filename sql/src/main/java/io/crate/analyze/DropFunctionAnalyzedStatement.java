/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.analyze;

import io.crate.types.DataType;

import java.util.List;

public class DropFunctionAnalyzedStatement implements DDLStatement {

    private final String schema;
    private final String name;
    private final boolean ifExists;
    private final List<DataType> argumentTypes;

    public DropFunctionAnalyzedStatement(String schema, String name, boolean ifExists, List<DataType> argumentTypes) {
        this.schema = schema;
        this.name = name;
        this.ifExists = ifExists;
        this.argumentTypes = argumentTypes;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitDropFunctionStatement(this, context);
    }

    public String name() {
        return name;
    }

    public String schema() {
        return schema;
    }

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
