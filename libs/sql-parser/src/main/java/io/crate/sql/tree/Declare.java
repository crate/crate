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

public final class Declare extends Statement {

    public enum Hold {
        WITH,
        WITHOUT
    }

    private final String cursorName;
    private final Hold hold;
    private final boolean binary;
    private final boolean scroll;
    private final Query query;

    public Declare(String cursorName,
                   Hold hold,
                   boolean binary,
                   boolean scroll,
                   Query query) {
        this.cursorName = cursorName;
        this.hold = hold;
        this.binary = binary;
        this.scroll = scroll;
        this.query = query;
    }

    public String cursorName() {
        return cursorName;
    }

    public Hold hold() {
        return hold;
    }

    public boolean binary() {
        return binary;
    }

    public boolean scroll() {
        return scroll;
    }

    public Query query() {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDeclare(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(binary, cursorName, hold, query, scroll);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Declare other = (Declare) obj;
        return binary == other.binary && Objects.equals(cursorName, other.cursorName) && hold == other.hold
                && Objects.equals(query, other.query) && scroll == other.scroll;
    }

    @Override
    public String toString() {
        return "Declare{binary=" + binary + ", cursorName=" + cursorName + ", hold=" + hold + ", query=" + query
                + ", scroll=" + scroll + "}";
    }
}
