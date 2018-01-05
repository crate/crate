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

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

public class CreateBlobTable extends Statement {

    private final Table name;
    @Nullable
    private final ClusteredBy clusteredBy;
    private final GenericProperties genericProperties;

    public CreateBlobTable(Table name, @Nullable ClusteredBy clusteredBy, GenericProperties properties) {
        this.name = name;
        this.clusteredBy = clusteredBy;
        this.genericProperties = properties;
    }

    public Table name() {
        return name;
    }

    @Nullable
    public ClusteredBy clusteredBy() {
        return clusteredBy;
    }

    public GenericProperties genericProperties() {
        return genericProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreateBlobTable that = (CreateBlobTable) o;

        if (clusteredBy != null ? !clusteredBy.equals(that.clusteredBy) : that.clusteredBy != null) return false;
        if (!genericProperties.equals(that.genericProperties)) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        if (clusteredBy != null) {
            result = 31 * result + clusteredBy.hashCode();
        }
        result = 31 * result + genericProperties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("clusteredBy", clusteredBy)
            .add("properties", genericProperties).toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateBlobTable(this, context);
    }
}
