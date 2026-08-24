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

import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

public record Window(@Nullable String windowRef,
                     List<Expression> partitions,
                     List<SortItem> orderBy,
                     Optional<WindowFrame> windowFrame) implements Statement {

    /**
     * Merges the provided window definition into the current one
     * by following the next merge rules:
     * <ul>
     * <li> The current window must not specify the partition by clause.
     * <li> The provided window must not specify the window frame, if
     *      the current window definition is not empty.
     * <li> The provided window cannot override the order by clause
     *      or window frame.
     * <ul/>
     *
     * @return A new {@link Window} window definition that contains merged
     *         elements of both current and provided windows or a provided
     *         window definition if the current definition is empty.
     * @throws IllegalArgumentException If the merge rules are violated.
     */
    public Window merge(Window that) {
        if (this.empty()) {
            return that;
        }

        final List<Expression> partitionBy;
        if (!this.partitions.isEmpty()) {
            throw new IllegalArgumentException(
                "Cannot override PARTITION BY clause of window " + this.windowRef);
        } else {
            partitionBy = that.partitions();
        }

        final List<SortItem> orderBy;
        if (that.orderBy().isEmpty()) {
            orderBy = this.orderBy();
        } else {
            if (!this.orderBy().isEmpty()) {
                throw new IllegalArgumentException(
                    "Cannot override ORDER BY clause of window " + this.windowRef);
            }
            orderBy = that.orderBy();
        }

        if (that.windowFrame().isPresent()) {
            throw new IllegalArgumentException(
                "Cannot copy window " + this.windowRef() + " because it has a frame clause");
        }

        return new Window(that.windowRef, partitionBy, orderBy, this.windowFrame());
    }

    private boolean empty() {
        return partitions.isEmpty() && orderBy.isEmpty() && windowFrame.isEmpty();
    }

    @Override
    public String toString() {
        return "Window{" +
               "partitions=" + partitions +
               ", orderBy=" + orderBy +
               ", windowFrame=" + windowFrame +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWindow(this, context);
    }
}
