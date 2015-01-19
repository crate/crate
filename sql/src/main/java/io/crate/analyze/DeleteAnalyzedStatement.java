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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.analyze.relations.AnalyzedRelation;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DeleteAnalyzedStatement extends AnalyzedStatement {

    private static final Predicate<WhereClause> HAS_NO_RESULT_PREDICATE = new Predicate<WhereClause>() {
        @Override
        public boolean apply(@Nullable WhereClause input) {
            return input != null && input.noMatch();
        }
    };

    final List<WhereClause> whereClauses = new ArrayList<>();
    final AnalyzedRelation analyzedRelation;

    public DeleteAnalyzedStatement(ParameterContext parameterContext, AnalyzedRelation analyzedRelation) {
        super(parameterContext);
        this.analyzedRelation = analyzedRelation;
    }

    public AnalyzedRelation analyzedRelation() {
        return analyzedRelation;
    }

    @Override
    public boolean expectsAffectedRows() {
        return true;
    }

    public List<WhereClause> whereClauses() {
        return whereClauses;
    }

    @Override
    public boolean hasNoResult() {
        return Iterables.all(whereClauses, HAS_NO_RESULT_PREDICATE);
    }

    @Override
    public void normalize() {
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitDeleteStatement(this, context);
    }
}
