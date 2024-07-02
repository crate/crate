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

import java.util.Objects;
import java.util.function.Function;

public class AlterRole<T> extends Statement {

    private final GenericProperties<T> properties;
    private final String name;

    public AlterRole(String name, GenericProperties<T> properties) {
        this.properties = properties;
        this.name = name;
    }

    public GenericProperties<T> properties() {
        return properties;
    }

    public String name() {
        return name;
    }

    public <U> AlterRole<U> map(Function<? super T, ? extends U> mapper) {
        return new AlterRole<>(name, properties.map(mapper));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AlterRole<?> alterRole = (AlterRole<?>) o;
        return Objects.equals(properties, alterRole.properties) &&
               Objects.equals(name, alterRole.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, name);
    }

    @Override
    public String toString() {
        return "AlterRole{" +
               "properties=" + properties +
               ", name='" + name + '\'' +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterRole(this, context);
    }
}
