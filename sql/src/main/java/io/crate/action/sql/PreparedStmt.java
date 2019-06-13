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

package io.crate.action.sql;

import com.google.common.base.Preconditions;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.ParamTypeHints;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class PreparedStmt {

    private final Statement parsedStatement;
    private final String rawStatement;
    private final ParamTypeHints paramTypes;

    @Nullable
    private DataType[] describedParameterTypes;

    @Nullable
    private AnalyzedStatement unboundStatement = null;
    private boolean relationInitialized = false;

    PreparedStmt(Statement parsedStatement, String query, List<DataType> paramTypes) {
        this.parsedStatement = parsedStatement;
        this.rawStatement = query;
        this.paramTypes = new ParamTypeHints(paramTypes);
    }

    /**
     * Sets the parameters sent back from a ParameterDescription message.
     * @param describedParameters The parameters in sorted order.
     */
    void setDescribedParameters(DataType[] describedParameters) {
        this.describedParameterTypes = describedParameters;
    }

    public Statement parsedStatement() {
        return parsedStatement;
    }

    ParamTypeHints paramTypes() {
        return paramTypes;
    }

    /**
     * Gets the list of effective parameter types which might be a combination
     * of the {@link ParamTypeHints} and the types determined during ParameterDescription.
     * @param idx type at index (zero-based).
     */
    DataType getEffectiveParameterType(int idx) {
        if (describedParameterTypes == null) {
            return paramTypes.getType(idx);
        }
        Preconditions.checkState(idx < describedParameterTypes.length,
            "Requested parameter index exceeds the number of parameters: " + idx);
        return describedParameterTypes[idx];
    }

    public String rawStatement() {
        return rawStatement;
    }

    boolean isRelationInitialized() {
        return relationInitialized;
    }

    @Nullable
    public AnalyzedStatement unboundStatement() {
        return unboundStatement;
    }

    /**
     * Set the unbound analyzed statement (or null if unbound analysis is not supported)
     *
     * This must not be set to a bound statement because a `preparedStmt` instance can be re-used
     * for multiple `bind` messages (so there would be different parameters)
     */
    public void unboundStatement(@Nullable AnalyzedStatement unboundStatement) {
        relationInitialized = true;
        this.unboundStatement = unboundStatement;
    }
}
