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

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class CurrentTime extends Expression {

    private final Type type;
    @Nullable
    private final Integer precision;

    public enum Type {
        TIME,
        DATE,
        TIMESTAMP
    }

    public CurrentTime(Type type) {
        this(type, null);
    }

    public CurrentTime(Type type, @Nullable Integer precision) {
        checkNotNull(type, "type is null");
        this.type = type;
        this.precision = precision;
    }

    public Type getType() {
        return type;
    }

    @Nullable
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCurrentTime(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CurrentTime that = (CurrentTime) o;

        if (precision != null ? !precision.equals(that.precision) : that.precision != null) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        if (precision != null) {
            result = 31 * result + precision.hashCode();
        }
        return result;
    }
}
