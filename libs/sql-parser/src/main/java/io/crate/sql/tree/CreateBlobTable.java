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

import java.util.Optional;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

public record CreateBlobTable<T>(Table<T> name,
                                 @Nullable ClusteredBy<T> clusteredBy,
                                 GenericProperties<T> genericProperties) implements Statement {

    public CreateBlobTable(Table<T> name, Optional<ClusteredBy<T>> clusteredBy, GenericProperties<T> properties) {
        this(name, clusteredBy.orElse(null), properties);
    }

    public <U> CreateBlobTable<U> map(Function<? super T, ? extends U> mapper) {
        return new CreateBlobTable<>(
            name.map(mapper),
            clusteredBy == null ? null : clusteredBy.map(mapper),
            genericProperties.map(mapper)
        );
    }

    @Override
    public String toString() {
        return "CreateBlobTable{" +
               "name=" + name +
               ", clusteredBy=" + clusteredBy +
               ", properties=" + genericProperties +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateBlobTable(this, context);
    }
}
