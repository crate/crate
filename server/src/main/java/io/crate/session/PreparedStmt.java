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

package io.crate.session;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.ParamTypeHints;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

public class PreparedStmt {

    private final AnalyzedStatement analyzedStatement;
    private final Statement parsedStatement;
    private final String rawStatement;
    private final DataType<?>[] describedParameterTypes;

    PreparedStmt(Statement parsedStatement,
                 AnalyzedStatement analyzedStatement,
                 String query,
                 DataType<?>[] parameterTypes) {
        this.parsedStatement = parsedStatement;
        this.analyzedStatement = analyzedStatement;
        this.rawStatement = query;
        this.describedParameterTypes = parameterTypes;
    }

    public AnalyzedStatement analyzedStatement() {
        return analyzedStatement;
    }


    Statement parsedStatement() {
        return parsedStatement;
    }

    DataType<?>[] parameterTypes() {
        return describedParameterTypes;
    }

    /**
     * Gets the list of effective parameter types which might be a combination
     * of the {@link ParamTypeHints} and the types determined during ParameterDescription.
     * @param idx type at index (zero-based).
     */
    DataType<?> getEffectiveParameterType(int idx) {
        if (idx >= describedParameterTypes.length) {
            throw new IllegalArgumentException("Requested parameter index exceeds the number of parameters: " + idx);
        }
        return describedParameterTypes[idx];
    }

    public String rawStatement() {
        return rawStatement;
    }
}
