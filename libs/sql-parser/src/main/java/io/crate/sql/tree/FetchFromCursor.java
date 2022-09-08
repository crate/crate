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

import javax.annotation.Nullable;

public class FetchFromCursor extends Cursor {

    enum Selector {
        FORWARD, BACKWARD, RELATIVE;
    }

    private final int count;
    private final Selector selector;

    public FetchFromCursor(int count, String cursorName, @Nullable String selector) {
        super(cursorName);
        this.count = count;
        this.selector = selector == null ? Selector.FORWARD : Selector.valueOf(selector);
    }

    public FetchFromCursor(String cursorName, @Nullable String selector) {
        this(1, cursorName, selector);
    }

    public Selector selector() {
        return selector;
    }

    public int count() {
        return count;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFetchFromCursor(this, context);
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
        return null;
    }
}
