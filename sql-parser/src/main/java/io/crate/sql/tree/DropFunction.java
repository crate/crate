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

public class DropFunction extends Statement {

    private final QualifiedName name;
    private final boolean exists;
    private final List<FunctionArgument> arguments;

    public DropFunction(QualifiedName name, boolean exists, List<FunctionArgument> arguments) {
        this.name = name;
        this.exists = exists;
        this.arguments = arguments;
    }

    public QualifiedName name() {
        return name;
    }

    public boolean exists() {
        return exists;
    }

    public List<FunctionArgument> arguments() {
        return arguments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFunction(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DropFunction that = (DropFunction) o;

        if (exists != that.exists) return false;
        if (!name.equals(that.name)) return false;
        return arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (exists ? 1 : 0);
        result = 31 * result + arguments.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DropFunction{" +
            "name=" + name +
            ", exists=" + exists +
            ", arguments=" + arguments +
            '}';
    }
}
