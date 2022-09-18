/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class DeclareCursor extends Statement {

    private final String cursorName;
    private final Query query;
    private final boolean isBinary;
    private final boolean isAsensitive;
    private final boolean isScroll;
    private final boolean isWithHold;

    public DeclareCursor(String cursorName, Query query, boolean isBinary, boolean isAsensitive, boolean isScroll, boolean isWithHold) {
        this.cursorName = cursorName;
        this.query = query;
        this.isBinary = isBinary;
        this.isAsensitive = isAsensitive;
        this.isScroll = isScroll;
        this.isWithHold = isWithHold;
    }

    public Query getQuery() {
        return query;
    }

    public String getCursorName() {
        return cursorName;
    }

    public boolean isBinary() {
        return isBinary;
    }

    public boolean isAsensitive() {
        return isAsensitive;
    }

    public boolean isScroll() {
        return isScroll;
    }

    public boolean isWithHold() {
        return isWithHold;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDeclareCursor(this, context);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public String toString() {
        return "DeclareCursor{" +
               "cursorName=" + getCursorName() +
               ", query=" + query +
               '}';
    }
}
