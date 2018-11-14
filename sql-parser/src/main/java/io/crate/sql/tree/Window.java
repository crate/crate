/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.tree;

import java.util.List;
import java.util.Optional;

public final class Window extends Node {

    private final List<Expression> partitions;
    private final List<SortItem> orderBy;
    private final Optional<WindowFrame> windowFrame;

    public Window(List<Expression> partitions, List<SortItem> orderBy, Optional<WindowFrame> windowFrame) {
        this.partitions = partitions;
        this.orderBy = orderBy;
        this.windowFrame = windowFrame;
    }

    public List<Expression> getPartitions() {
        return partitions;
    }

    public List<SortItem> getOrderBy() {
        return orderBy;
    }

    public Optional<WindowFrame> getWindowFrame() {
        return windowFrame;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Window window = (Window) o;

        if (!partitions.equals(window.partitions)) return false;
        if (!orderBy.equals(window.orderBy)) return false;
        return windowFrame.equals(window.windowFrame);
    }

    @Override
    public int hashCode() {
        int result = partitions.hashCode();
        result = 31 * result + orderBy.hashCode();
        result = 31 * result + windowFrame.hashCode();
        return result;
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
