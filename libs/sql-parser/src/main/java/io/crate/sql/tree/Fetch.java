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

public final class Fetch extends Statement {

    /**
     * Normalized direction
     *
     * <pre>
     * {@code
     *      NEXT                        relative +1
     *      PRIOR                       relative -1
     *      FIRST                       absolute 1
     *      LAST                        absolute -1
     *      ABSOLUTE integerLiteral     absolute <count>
     *      RELATIVE integerLiteral     relative <count>
     *      integerLiteral              relative <count>
     *      ALL                         relative Long.MAX_VALUE
     *      FORWARD                     relative 1
     *      FORWARD integerLiteral      relative <count>
     *      FORWARD ALL                 relative Long.MAX_VALUE
     *      BACKWARD                    relative -1
     *      BACKWARD integerLiteral     relative (<count> * - 1)
     *      BACKWARD ALL                relative - Long.MAX_VALUE
     * }
     * </pre>
     */
    public enum ScrollMode {
        RELATIVE,
        ABSOLUTE
    }

    private final ScrollMode scrollMode;
    private final long count;
    private final String cursorName;

    /**
     * @param scollMode normalized direction. See {@link ScrollMode}
     */
    public Fetch(ScrollMode scollMode, long count, String cursorName) {
        this.scrollMode = scollMode;
        this.count = count;
        this.cursorName = cursorName;
    }

    public ScrollMode scrollMode() {
        return scrollMode;
    }

    public long count() {
        return count;
    }

    public String cursorName() {
        return cursorName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFetch(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, cursorName, scrollMode);
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
        Fetch other = (Fetch) obj;
        return count == other.count && Objects.equals(cursorName, other.cursorName) && scrollMode == other.scrollMode;
    }

    @Override
    public String toString() {
        return "Fetch{count=" + count + ", cursorName=" + cursorName + ", scrollMode=" + scrollMode + "}";
    }
}
