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

package org.cratedb.action.collect;

import org.cratedb.DataType;

import java.util.Map;

public class ObjectColumnReference<ReturnType> extends ColumnReferenceCollectorExpression<Map<String, Object>> {

    public ObjectColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Map<String, Object> evaluate() {
        //TODO: this returns null since we are called in aggregation collectors onnly vor now,
        // once this gets called from result columns it should be implemented
        return null;
    }

    @Override
    public DataType returnType() {
        return DataType.OBJECT;
    }

}
