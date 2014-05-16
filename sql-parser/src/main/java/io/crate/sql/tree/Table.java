/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class Table
        extends QueryBody
{
    private final QualifiedName name;
    private final Optional<GenericProperties> partitionProperties;

    public Table(QualifiedName name)
    {
        this.name = name;
        this.partitionProperties = Optional.absent();
    }

    public Table(QualifiedName name, GenericProperties partitionProperties) {
        this.name = name;
        this.partitionProperties = Optional.fromNullable(partitionProperties);
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<GenericProperties> partitionProperties() {
        return partitionProperties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTable(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(name)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Table)) return false;

        Table table = (Table) o;

        if (!name.equals(table.name)) return false;
        if (!partitionProperties.equals(table.partitionProperties)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + partitionProperties.hashCode();
        return result;
    }
}
