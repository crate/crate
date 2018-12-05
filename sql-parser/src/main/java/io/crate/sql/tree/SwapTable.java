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

public final class SwapTable extends Statement {

    private final QualifiedName source;
    private final QualifiedName target;
    private final GenericProperties properties;

    public SwapTable(QualifiedName source, QualifiedName target, GenericProperties properties) {
        this.source = source;
        this.target = target;
        this.properties = properties;
    }

    public QualifiedName source() {
        return source;
    }

    public QualifiedName target() {
        return target;
    }

    public GenericProperties properties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSwapTable(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwapTable swapTable = (SwapTable) o;

        if (!source.equals(swapTable.source)) return false;
        if (!target.equals(swapTable.target)) return false;
        return properties.equals(swapTable.properties);
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + target.hashCode();
        result = 31 * result + properties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwapTable{" +
               "source=" + source +
               ", target=" + target +
               ", properties=" + properties +
               '}';
    }
}
