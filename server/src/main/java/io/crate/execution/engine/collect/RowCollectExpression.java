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

import io.crate.data.Row;

/**
 * CollectExpression to retrieve values from a {@link Row} at a given index
 */
public final class RowCollectExpression implements CollectExpression<Row, Object> {

    private final int index;
    private Object value;

    public RowCollectExpression(int index) {
        this.index = index;
    }

    @Override
    public void setNextRow(Row row) {
        assert row.numColumns() > index
            : "Wanted to retrieve value for column at position=" + index + " from row=" + row + " but row has only " + row.numColumns() + " columns";
        value = row.get(index);
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowCollectExpression that = (RowCollectExpression) o;

        if (index != that.index) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RowCollectExpression{idx=" + index + '}';
    }
}
