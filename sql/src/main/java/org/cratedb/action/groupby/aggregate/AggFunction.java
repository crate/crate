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

package org.cratedb.action.groupby.aggregate;

import org.cratedb.DataType;

import java.util.Collection;

public abstract class AggFunction<T extends AggState> {

    public abstract String name();

    /**
     * Apply the columnValue to the argument AggState using the logic in this AggFunction
     *
     * @param state the state to apply the columnValue on
     * @param columnValue the columnValue found in a document
     * @return false if we do not need any further iteration for this state
     */
    public abstract boolean iterate(T state, Object columnValue);
    public abstract Collection<DataType> supportedColumnTypes();
    public boolean supportsDistinct() {
        return false;
    }
}
