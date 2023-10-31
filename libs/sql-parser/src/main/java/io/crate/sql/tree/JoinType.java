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

public enum JoinType {
    CROSS,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    SEMI,
    ANTI;

    public JoinType invert() {
        return switch (this) {
            case CROSS -> CROSS;
            case INNER -> INNER;
            case LEFT -> RIGHT;
            case RIGHT -> LEFT;
            case FULL -> FULL;
            case SEMI -> throw new UnsupportedOperationException("SEMI Join cannot be inverted");
            case ANTI -> throw new UnsupportedOperationException("ANTI Join cannot be inverted");
        };
    }

    public boolean supportsInversion() {
        return switch (this) {
            case SEMI, ANTI -> false;
            default -> true;
        };
    }

    public boolean isOuter() {
        return switch (this) {
            case LEFT, RIGHT, FULL -> true;
            default -> false;
        };
    }
}
