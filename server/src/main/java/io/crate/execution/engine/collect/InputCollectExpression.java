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

package io.crate.execution.engine.collect;

import io.crate.data.Row;

public class InputCollectExpression implements CollectExpression<Row, Object> {

    private final int position;
    private Row row = null;

    public InputCollectExpression(int position) {
        this.position = position;
    }

    @Override
    public void setNextRow(Row row) {
        assert row.numColumns() > position
            : "Wanted to retrieve value for column at position=" + position + " from row=" + row + " but row has only " + row.numColumns() + " columns";
        this.row = row;
    }

    @Override
    public Object value() {
        if (row == null) {
            return null;
        }
        return row.get(position);
    }

    @Override
    public double getDouble() {
        return row.getDouble(position);
    }

    @Override
    public long getLong() {
        return row.getLong(position);
    }

    @Override
    public boolean hasValue() {
        if (row == null) {
            return false;
        }
        return row.hasValue(position);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InputCollectExpression that = (InputCollectExpression) o;

        if (position != that.position) return false;
        if (row != null ? !row.equals(that.row) : that.row != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = position;
        result = 31 * result + (row != null ? row.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Input{pos=" + position + '}';
    }
}
