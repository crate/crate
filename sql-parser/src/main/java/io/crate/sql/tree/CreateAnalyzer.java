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
import com.google.common.base.Optional;

import javax.annotation.Nullable;
import java.util.List;

public class CreateAnalyzer extends Statement {

    private final String ident;
    private final Optional<String> extendedAnalyzer;
    private final List<AnalyzerElement> elements;
    private final GenericProperties properties;

    public CreateAnalyzer(String ident, @Nullable String extendedAnalyzer, List<AnalyzerElement> elements) {
        this.ident = ident;
        this.extendedAnalyzer = Optional.fromNullable(extendedAnalyzer);
        this.elements = elements;

        this.properties = new GenericProperties();
        for (AnalyzerElement element : elements) {
            if (element instanceof GenericProperty) {
                this.properties.add((GenericProperty) element);
            }
        }

    }

    public String ident() {
        return ident;
    }

    public Optional<String> extendedAnalyzer() {
        return extendedAnalyzer;
    }

    public boolean isExtending() {
        return extendedAnalyzer.isPresent();
    }

    public List<AnalyzerElement> elements() {
        return elements;
    }

    /**
     * if the statement contains any {@link io.crate.sql.tree.GenericProperty}s
     * they will be gathered and returned as an instance of {@link io.crate.sql.tree.GenericProperties}.
     */
    public GenericProperties getProperties() {
        return this.properties;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ident, extendedAnalyzer, elements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreateAnalyzer that = (CreateAnalyzer) o;

        if (!elements.equals(that.elements)) return false;
        if (!extendedAnalyzer.equals(that.extendedAnalyzer)) return false;
        if (!ident.equals(that.ident)) return false;
        if (!properties.equals(that.properties)) return false;

        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("ident", ident)
                .add("extends", extendedAnalyzer)
                .add("elements", elements)
                .add("properties", properties)
                .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAnalyzer(this, context);
    }
}
