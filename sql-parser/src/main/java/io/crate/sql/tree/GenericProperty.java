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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * A key-value entry mapping a string to a list of <code>Expression</code>s.
 * A <code>GenericProperty</code> always belongs to {@link io.crate.sql.tree.GenericProperties}.
 * <p>
 * It does not need to be visited.
 * Values are merged into {@link io.crate.sql.tree.GenericProperties}.
 * <p>
 * Instance of {@link io.crate.sql.tree.AnalyzerElement} but frequently used in other
 * {@link io.crate.sql.tree.GenericProperties} contexts.
 */
public class GenericProperty<T> extends AnalyzerElement {

    private final String key;
    private final T value;

    public GenericProperty(String key, T value) {
        this.key = key;
        this.value = value;
    }


    public String key() {
        return key;
    }

    public T value() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenericProperty that = (GenericProperty) o;

        if (!key.equals(that.key)) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("key", key)
            .add("value", value)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGenericProperty(this, context);
    }
}
