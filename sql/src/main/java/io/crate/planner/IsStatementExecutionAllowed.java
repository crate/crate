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

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.SetLicenseAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;

import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

final class IsStatementExecutionAllowed implements Predicate<AnalyzedStatement> {

    private static final IsReadQueryOnSystemTable isReadQueryOnSystemTable = new IsReadQueryOnSystemTable();
    private final BooleanSupplier hasValidLicense;

    IsStatementExecutionAllowed(BooleanSupplier hasValidLicense) {
        this.hasValidLicense = hasValidLicense;
    }

    @Override
    public boolean test(AnalyzedStatement analyzedStatement) {
        if (hasValidLicense.getAsBoolean()) {
            return true;
        }

        if (analyzedStatement instanceof SetLicenseAnalyzedStatement) {
            return true;
        }

        return (analyzedStatement instanceof QueriedRelation
                && isReadQueryOnSystemTable.test((QueriedRelation) analyzedStatement));
    }

    private static final class IsReadQueryOnSystemTable extends AnalyzedRelationVisitor<Void, Boolean> implements Predicate<AnalyzedRelation> {

        private static boolean isSysSchema(String schema) {
            return SysSchemaInfo.NAME.equals(schema)
                   || InformationSchemaInfo.NAME.equals(schema);
        }

        @Override
        public boolean test(AnalyzedRelation relation) {
            return process(relation, null);
        }

        @Override
        public Boolean visitUnionSelect(UnionSelect unionSelect, Void context) {
            return process(unionSelect.left(), context) && process(unionSelect.right(), context);
        }

        @Override
        public Boolean visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Void context) {
            Boolean isAllowed = true;
            for (AnalyzedRelation relation : multiSourceSelect.sources().values()) {
                isAllowed &= process(relation, context);
                if (!isAllowed) break;
            }
            return isAllowed;
        }

        @Override
        public Boolean visitOrderedLimitedRelation(OrderedLimitedRelation relation, Void context) {
            return process(relation.childRelation(), context);
        }

        @Override
        public Boolean visitQueriedSelectRelation(QueriedSelectRelation relation, Void context) {
            return process(relation.subRelation(), context);
        }

        @Override
        public Boolean visitQueriedTable(QueriedTable<?> queriedTable, Void context) {
            return isSysSchema(queriedTable.tableRelation().tableInfo().ident().schema());
        }
    }
}
