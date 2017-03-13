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

package io.crate.operation.projectors.fetch;

import io.crate.data.Row;

/**
 * An array backed row, which returns the inner array upon materialize
 */
public class ArrayBackedRow implements Row {

    Object[] cells;

    @Override
    public int numColumns() {
        return cells.length;
    }

    @Override
    public Object get(int index) {
        assert cells != null : "cells must not be null";
        return cells[index];
    }

    @Override
    public Object[] materialize() {
        return cells;
    }
}
