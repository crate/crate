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

import java.util.ArrayList;
import java.util.List;

import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;

/**
 * Holds information about a parsed subscript expression
 */
public class SubscriptContext {

    private QualifiedName qName;

    private boolean hasExpression;

    private final List<String> parts = new ArrayList<>();

    private final List<Expression> index = new ArrayList<>();

    public SubscriptContext() {
    }

    public QualifiedName qualifiedName() {
        return qName;
    }

    public void qualifiedName(QualifiedName qName) {
        this.qName = qName;
        assert this.hasExpression == false;
    }

    public List<String> parts() {
        return parts;
    }

    public void addKey(String part) {
        parts.add(0, part);
    }

    public void addIndex(Expression index) {
        this.index.add(0, index);
    }

    public List<Expression> index() {
        return index;
    }

    public void expression(Expression expression) {
        assert this.qName == null;
        this.hasExpression = true;
    }

    public boolean hasExpression() {
        return this.hasExpression;
    }

}
