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

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

class PreparedStmt {

    private final Statement statement;
    private final String query;
    private final ParamTypeHints paramTypes;

    private AnalyzedRelation relation;
    private boolean relationInitialized = false;

    PreparedStmt(Statement statement, String query, List<DataType> paramTypes) {
        this.statement = statement;
        this.query = query;
        this.paramTypes = new ParamTypeHints(paramTypes);
    }

    public Statement statement() {
        return statement;
    }

    ParamTypeHints paramTypes() {
        return paramTypes;
    }

    public String query() {
        return query;
    }

    public boolean isRelationInitialized() {
        return relationInitialized;
    }

    @Nullable
    public AnalyzedRelation relation() {
        return relation;
    }

    public void relation(@Nullable AnalyzedRelation relation) {
        relationInitialized = true;
        this.relation = relation;
    }
}
