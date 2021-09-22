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

package io.crate.exceptions;

import io.crate.metadata.RelationName;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

public class ColumnUnknownException extends ResourceUnknownException implements TableScopeException {

    private final RelationName relationName;

    private ColumnUnknownException(String message) {
        super(message);
        this.relationName = null;
    }

    public ColumnUnknownException(String columnName, @Nonnull RelationName relationName) {
        super(String.format(Locale.ENGLISH, "Column %s unknown", columnName));
        this.relationName = Objects.requireNonNull(relationName);
    }

    /**
     * <p>
     * CAUTION: not providing the relationName may prevent permission checks on the user and
     * leak unprivileged information through the given message.
     * </p>
     * <code>
     * ex) select '{"x":10}'::object['y'];<br>
     * ColumnUnknownException[The object `{x=10}` does not contain the key `y`]<br>
     * // The column is unnamed and there is no associated table that requires permission checks. This is the only usage currently.
     * </code>
     */
    public static ColumnUnknownException ofUnknownRelation(String message) {
        return new ColumnUnknownException(message);
    }

    @Override
    public Iterable<RelationName> getTableIdents() {
        return (relationName == null) ? Collections.emptyList() : Collections.singletonList(relationName);
    }
}
