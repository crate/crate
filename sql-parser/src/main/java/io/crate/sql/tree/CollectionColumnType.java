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

import java.util.Locale;

/**
 * columntype that contains many values of a single type
 */
public class CollectionColumnType extends ColumnType {

    private final ColumnType innerType;

    public static CollectionColumnType array(ColumnType innerType) {
        return new CollectionColumnType(innerType, Type.ARRAY);
    }

    public static CollectionColumnType set(ColumnType innerType) {
        return new CollectionColumnType(innerType, Type.SET);
    }

    private CollectionColumnType(ColumnType innerType, Type type) {
        super(type.name().toUpperCase(Locale.ENGLISH), type);
        this.innerType = innerType;
    }

    public ColumnType innerType() {
        return innerType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, type, innerType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CollectionColumnType that = (CollectionColumnType) o;

        if (!innerType.equals(that.innerType)) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCollectionColumnType(this, context);
    }
}
