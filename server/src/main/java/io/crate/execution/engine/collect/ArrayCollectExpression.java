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

package io.crate.execution.engine.collect;

import java.util.Objects;

/**
 * CollectExpression to retrieve values from a object array at a given index
 */
public final class ArrayCollectExpression implements CollectExpression<Object[], Object> {

    private final int index;
    private Object value;

    public ArrayCollectExpression(int index) {
        assert index > -1 : "Index must be positive: " + index;
        this.index = index;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void setNextRow(Object[] row) {
        value = row[index];
    }

    @Override
    public int hashCode() {
        return 31 * index + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArrayCollectExpression other = (ArrayCollectExpression) obj;
        return index == other.index && Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        return "ArrayCollectExpression{idx=" + index + '}';
    }
}
