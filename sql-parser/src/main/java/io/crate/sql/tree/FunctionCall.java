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

package io.crate.sql.tree;

import java.util.List;
import java.util.Optional;

public class FunctionCall extends Expression {

    private final QualifiedName name;
    private final boolean distinct;
    private final List<Expression> arguments;
    private final Optional<Window> window;

    public FunctionCall(QualifiedName name, List<Expression> arguments) {
        this(name, false, arguments, Optional.empty());
    }

    public FunctionCall(QualifiedName name, boolean distinct, List<Expression> arguments, Optional<Window> window) {
        this.name = name;
        this.distinct = distinct;
        this.arguments = arguments;
        this.window = window;
    }

    public QualifiedName getName() {
        return name;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public List<Expression> getArguments() {
        return arguments;
    }

    public Optional<Window> getWindow() {
        return window;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFunctionCall(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FunctionCall that = (FunctionCall) o;

        if (distinct != that.distinct) return false;
        if (!name.equals(that.name)) return false;
        if (!arguments.equals(that.arguments)) return false;
        return window.equals(that.window);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (distinct ? 1 : 0);
        result = 31 * result + arguments.hashCode();
        result = 31 * result + window.hashCode();
        return result;
    }
}
