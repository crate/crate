/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

public class AlterServer<T> extends Statement {

    public enum Operation {
        ADD,
        SET,
        DROP
    }

    public record Option<T>(Operation operation, String key, @Nullable T value) {}


    private final String name;
    private final List<Option<T>> options;

    public AlterServer(String name,
                       List<Option<T>> options) {
        this.name = name;
        this.options = options;
    }

    public String name() {
        return name;
    }


    public List<Option<T>> options() {
        return options;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AlterServer<?> other = (AlterServer<?>) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(options, other.options);
    }

    @Override
    public String toString() {
        return "AlterServer{name=" + name + "}";
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterServer(this, context);
    }
}
