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

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.function.Function;

public class AlterUser<T> extends Statement {

    private final GenericProperties<T> properties;
    private final String name;

    public AlterUser(String name, GenericProperties<T> properties) {
        this.properties = properties;
        this.name = name;
    }

    public GenericProperties<T> properties() {
        return properties;
    }

    public String name() {
        return name;
    }

    public <U> AlterUser<U> map(Function<? super T, ? extends U> mapper) {
        return new AlterUser<>(name, properties.map(mapper));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlterUser alterUser = (AlterUser) o;

        if (!properties.equals(alterUser.properties)) return false;
        return name.equals(alterUser.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("properties", properties)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterUser(this, context);
    }
}
