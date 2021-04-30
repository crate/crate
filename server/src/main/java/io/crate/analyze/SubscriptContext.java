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

package io.crate.analyze;

import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SubscriptContext {

    private QualifiedName qName;

    private Expression expression;

    private final List<String> parts = new ArrayList<>();
    private Expression index;

    public SubscriptContext() {
    }

    public QualifiedName qualifiedName() {
        return qName;
    }

    public void qualifiedName(QualifiedName qName) {
        this.qName = qName;
    }

    public List<String> parts() {
        return parts;
    }

    public void add(String part) {
        parts.add(0, part);
    }

    public void index(Expression index) {
        this.index = index;
    }

    @Nullable
    public Expression index() {
        return index;
    }

    @Nullable
    public Expression expression() {
        return expression;
    }

    public void expression(Expression expression) {
        this.expression = expression;
    }

}
