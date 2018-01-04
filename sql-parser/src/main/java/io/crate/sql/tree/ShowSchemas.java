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
import java.util.Objects;

public class ShowSchemas extends Statement {

    @Nullable
    private final String likePattern;
    @Nullable
    private final Expression whereExpression;

    public ShowSchemas(@Nullable String likePattern, @Nullable Expression whereExpr) {
        this.likePattern = likePattern;
        this.whereExpression = whereExpr;
    }

    @Nullable
    public String likePattern() {
        return likePattern;
    }

    @Nullable
    public Expression whereExpression() {
        return whereExpression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSchemas(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShowSchemas that = (ShowSchemas) o;
        return Objects.equals(likePattern, that.likePattern) &&
            Objects.equals(whereExpression, that.whereExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(likePattern, whereExpression);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("likePattern", likePattern)
            .add("whereExpression", whereExpression)
            .toString();
    }

}
