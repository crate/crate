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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 * A key-value entry mapping a string to a list of <code>Expression</code>s.
 * A <code>GenericProperty</code> always belongs to {@link io.crate.sql.tree.GenericProperties}.
 * <p>
 * It does not need to be visited.
 * Values are merged into {@link io.crate.sql.tree.GenericProperties}.
 */
public class GenericProperty extends Node {

    private final String key;
    private final List<Expression> value;

    public GenericProperty(String key, List<Expression> value) {
        this.key = key;
        this.value = Objects.firstNonNull(value, ImmutableList.<Expression>of());
    }

    public String key() {
        return key;
    }

    public List<Expression> value() {
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
        return Objects.toStringHelper(this)
                .add("key", key)
                .add("value", value)
                .toString();
    }
}
