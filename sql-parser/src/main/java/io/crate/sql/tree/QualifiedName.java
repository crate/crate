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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QualifiedName {

    private final List<String> parts;

    public static QualifiedName of(String first, String... rest) {
        Preconditions.checkNotNull(first, "first is null");
        return new QualifiedName(ImmutableList.copyOf(Lists.asList(first, rest)));
    }

    public static QualifiedName of(Iterable<String> parts) {
        Preconditions.checkNotNull(parts, "parts is null");
        Preconditions.checkArgument(!Iterables.isEmpty(parts), "parts is empty");

        return new QualifiedName(parts);
    }

    public QualifiedName(String name) {
        this(ImmutableList.of(name));
    }

    public QualifiedName(Iterable<String> parts) {
        Preconditions.checkNotNull(parts, "parts");
        Preconditions.checkArgument(!Iterables.isEmpty(parts), "parts is empty");

        this.parts = ImmutableList.copyOf(parts);
    }

    public List<String> getParts() {
        return parts;
    }

    @Override
    public String toString() {
        return Joiner.on('.').join(parts);
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix() {
        if (parts.size() == 1) {
            return Optional.empty();
        }

        return Optional.of(QualifiedName.of(parts.subList(0, parts.size() - 1)));
    }

    public String getSuffix() {
        return Iterables.getLast(parts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QualifiedName that = (QualifiedName) o;

        if (!parts.equals(that.parts)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return parts.hashCode();
    }

    public QualifiedName withPrefix(String prefix) {
        ArrayList<String> newParts = new ArrayList<>(parts.size() + 1);
        newParts.add(prefix);
        newParts.addAll(parts);
        return new QualifiedName(newParts);
    }
}
