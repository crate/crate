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

package io.crate.sql.tree;

import java.util.List;
import java.util.Objects;

public class CreateFunction extends Statement {

    private final QualifiedName name;
    private final boolean replace;
    private final List<FunctionArgument> arguments;
    private final ColumnType returnType;
    private final Expression language;
    private final Expression definition;

    public CreateFunction(QualifiedName name,
                          boolean replace,
                          List<FunctionArgument> arguments,
                          ColumnType returnType,
                          Expression language,
                          Expression definition) {
        this.name = name;
        this.replace = replace;
        this.arguments = arguments;
        this.returnType = returnType;
        this.language = language;
        this.definition = definition;
    }

    public QualifiedName name() {
        return name;
    }

    public boolean replace() {
        return replace;
    }

    public List<FunctionArgument> arguments() {
        return arguments;
    }

    public ColumnType returnType() {
        return returnType;
    }

    public Expression language() {
        return language;
    }

    public Expression definition() {
        return definition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateFunction(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final CreateFunction that = (CreateFunction) o;
        return Objects.equals(this.name, that.name)
            && Objects.equals(this.replace, that.replace)
            && Objects.equals(this.arguments, that.arguments)
            && Objects.equals(this.returnType, that.returnType)
            && Objects.equals(this.language, that.language);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, replace, arguments, returnType, language);
    }

    @Override
    public String toString() {
        return "CreateFunction{" +
            "name=" + name +
            ", replace=" + replace +
            ", arguments=" + arguments +
            ", returnType=" + returnType +
            ", language=" + language +
            ", definition=" + definition +
            '}';
    }

    @Override
    public PrivilegeType privilegeType() {
        return PrivilegeType.DDL;
    }

    @Override
    public String clazz() { return "SCHEMA";}
}
