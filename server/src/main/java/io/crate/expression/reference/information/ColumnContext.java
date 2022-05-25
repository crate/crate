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

package io.crate.expression.reference.information;

import io.crate.metadata.SimpleReference;
import io.crate.metadata.RelationInfo;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.StringType;

public class ColumnContext {

    public final RelationInfo tableInfo;
    public final SimpleReference info;

    public ColumnContext(RelationInfo tableInfo, SimpleReference ref) {
        this.tableInfo = tableInfo;
        this.info = ref;
    }

    public Integer characterMaximumLength() {
        DataType<?> type = info.valueType();
        if (type instanceof StringType stringType) {
            if (stringType.unbound()) {
                return null;
            } else {
                return stringType.lengthLimit();
            }
        } else if (type instanceof BitStringType bitStringType) {
            return bitStringType.length();
        } else {
            return null;
        }
    }
}
