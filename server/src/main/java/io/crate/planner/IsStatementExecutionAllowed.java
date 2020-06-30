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

package io.crate.planner;

import io.crate.analyze.AnalyzedDecommissionNode;
import io.crate.analyze.AnalyzedSetLicenseStatement;
import io.crate.analyze.AnalyzedSetStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;

import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

final class IsStatementExecutionAllowed implements Predicate<AnalyzedStatement> {

    private static final IsReadQueryOnTableRelationOrTableFunction IS_READ_QUERY_ON_SYS_TABLE_OR_TABLE_FUNCTION =
        new IsReadQueryOnTableRelationOrTableFunction();
    private final BooleanSupplier hasValidLicense;

    IsStatementExecutionAllowed(BooleanSupplier hasValidLicense) {
        this.hasValidLicense = hasValidLicense;
    }

    @Override
    public boolean test(AnalyzedStatement analyzedStatement) {
        if (hasValidLicense.getAsBoolean()) {
            return true;
        }

        if (analyzedStatement instanceof AnalyzedSetLicenseStatement ||
            analyzedStatement instanceof AnalyzedDecommissionNode) {
            return true;
        }
        if (analyzedStatement instanceof AnalyzedSetStatement) {
            switch (((AnalyzedSetStatement) analyzedStatement).scope()) {
                case SESSION:
                case LOCAL:
                    return true;

                default:
                    return false;
            }
        }
        return (analyzedStatement instanceof AnalyzedRelation
                && IS_READ_QUERY_ON_SYS_TABLE_OR_TABLE_FUNCTION.test((AnalyzedRelation) analyzedStatement));
    }

    private static final class IsReadQueryOnTableRelationOrTableFunction extends AnalyzedRelationVisitor<Void, Boolean> implements Predicate<AnalyzedRelation> {

        @Override
        public boolean test(AnalyzedRelation relation) {
            return relation.accept(this, null);
        }

        @Override
        protected Boolean visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return false;
        }

        @Override
        public Boolean visitUnionSelect(UnionSelect unionSelect, Void context) {
            return unionSelect.left().accept(this, context) && unionSelect.right().accept(this, context);
        }

        @Override
        public Boolean visitQueriedSelectRelation(QueriedSelectRelation relation, Void context) {
            for (AnalyzedRelation analyzedRelation : relation.from()) {
                if (!analyzedRelation.accept(this, context)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visitTableRelation(TableRelation tableRelation, Void context) {
            return true;
        }

        @Override
        public Boolean visitDocTableRelation(DocTableRelation relation, Void context) {
            return false;
        }

        @Override
        public Boolean visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, Void context) {
            return true;
        }

        @Override
        public Boolean visitView(AnalyzedView analyzedView, Void context) {
            return analyzedView.relation().accept(this, context);
        }
    }
}
