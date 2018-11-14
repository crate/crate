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

import java.util.Optional;

public class WindowFrame extends Node {

    public enum Type {
        RANGE,
        ROWS
    }

    private final Type frameType;
    private final FrameBound start;
    private final Optional<FrameBound> end;

    public WindowFrame(Type frameType, FrameBound start, Optional<FrameBound> end) {
        this.frameType = frameType;
        this.start = start;
        this.end = end;
    }

    public Type getType() {
        return frameType;
    }

    public FrameBound getStart() {
        return start;
    }

    public Optional<FrameBound> getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WindowFrame that = (WindowFrame) o;

        if (frameType != that.frameType) return false;
        if (!start.equals(that.start)) return false;
        return end.equals(that.end);
    }

    @Override
    public int hashCode() {
        int result = frameType.hashCode();
        result = 31 * result + start.hashCode();
        result = 31 * result + end.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "WindowFrame{" +
               "frameType=" + frameType +
               ", start=" + start +
               ", end=" + end +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWindowFrame(this, context);
    }
}
