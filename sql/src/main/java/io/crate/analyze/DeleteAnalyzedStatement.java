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

package io.crate.analyze;

import io.crate.analyze.relations.DocTableRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated This statement type is bulk-operation aware;
 *             we're moving parameter/bulk-awareness out of the analyzer because params are not
 *             always available for analysis (in the postgres protocol)
 *             Use {@link AnalyzedDeleteStatement} instead
 */
@Deprecated
public class DeleteAnalyzedStatement implements AnalyzedStatement {

    final List<WhereClause> whereClauses = new ArrayList<>();
    final DocTableRelation analyzedRelation;

    public DeleteAnalyzedStatement(DocTableRelation analyzedRelation) {
        this.analyzedRelation = analyzedRelation;
    }

    public DocTableRelation analyzedRelation() {
        return analyzedRelation;
    }

    public List<WhereClause> whereClauses() {
        return whereClauses;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitDeleteStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
