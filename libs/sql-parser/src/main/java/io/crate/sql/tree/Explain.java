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

import static java.util.Objects.requireNonNull;

public class Explain extends Statement {

    public enum Mode {
        ANALYZE,
        STATS,
        DEFAULT
    }

    private final Statement statement;
    private final Mode mode;

    public Explain(Statement statement, Mode mode) {
        this.statement = requireNonNull(statement, "statement is null");
        this.mode = mode;
    }

    public Statement getStatement() {
        return statement;
    }

    public Mode mode() {
        return mode;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExplain(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Explain explain = (Explain) o;
        return mode == explain.mode &&
               Objects.equals(statement, explain.statement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement, mode);
    }

    @Override
    public String toString() {
        return "Explain{" +
               "statement=" + statement +
               ", mode=" + mode +
               '}';
    }
}
