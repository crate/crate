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
import java.util.Objects;
import java.util.function.Function;

public class AlterTable<T> extends Statement {

    private final Table<T> table;
    private final GenericProperties<T> setProperties;
    private final List<String> resetProperties;
    private final GenericProperties<T> withProperties;

    public AlterTable(Table<T> table,
                      GenericProperties<T> setProperties,
                      List<String> resetProperties,
                      GenericProperties<T> withProperties) {
        this.table = table;
        this.setProperties = setProperties;
        this.resetProperties = resetProperties;
        this.withProperties = withProperties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTable(this, context);
    }

    public Table<T> table() {
        return table;
    }

    public GenericProperties<T> setProperties() {
        return setProperties;
    }

    public List<String> resetProperties() {
        return resetProperties;
    }

    public GenericProperties<T> withProperties() {
        return withProperties;
    }

    public <U> AlterTable<U> map(Function<? super T, ? extends U> mapper) {
        return new AlterTable<>(
            table.map(mapper),
            setProperties.map(mapper),
            resetProperties,
            withProperties.map(mapper)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AlterTable<?> that = (AlterTable<?>) o;
        return table.equals(that.table)
            && setProperties.equals(that.setProperties)
            && resetProperties.equals(that.resetProperties)
            && withProperties.equals(that.withProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, setProperties, resetProperties, withProperties);
    }

    @Override
    public String toString() {
        return "AlterTable{" +
               "table=" + table +
               ", set=" + setProperties +
               ", reset=" + resetProperties +
               ", with=" + withProperties +
               '}';
    }
}
