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
import io.crate.analyze.SetAnalyzedStatement;
import io.crate.analyze.SetLicenseAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.expression.tablefunctions.EmptyRowTableFunction;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.TableInfo;

import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

final class IsStatementExecutionAllowed implements Predicate<AnalyzedStatement> {

    private static final IsReadQueryOnSystemOrEmptyRowTable IS_READ_QUERY_ON_SYSTEM_TABLE = new IsReadQueryOnSystemOrEmptyRowTable();
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
        if (analyzedStatement instanceof SetAnalyzedStatement) {
            switch (((SetAnalyzedStatement) analyzedStatement).scope()) {
                case SESSION_TRANSACTION_MODE:
                case SESSION:
                case LOCAL:
                    return true;

                default:
                    return false;
            }
        }
        return (analyzedStatement instanceof QueriedRelation
                && IS_READ_QUERY_ON_SYSTEM_TABLE.test((QueriedRelation) analyzedStatement));
    }

    private static final class IsReadQueryOnSystemOrEmptyRowTable extends AnalyzedRelationVisitor<Void, Boolean> implements Predicate<AnalyzedRelation> {

        private static boolean isSysSchema(String schema) {
            return SysSchemaInfo.NAME.equals(schema)
                   || InformationSchemaInfo.NAME.equals(schema);
        }

        private static boolean isEmptyRowTable(TableInfo tableInfo) {
            return tableInfo.ident().equals(EmptyRowTableFunction.TABLE_IDENT);
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
            for (AnalyzedRelation relation : multiSourceSelect.sources().values()) {
                if (process(relation, context) == false) {
                    return false;
                }
            }
            return true;
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
            TableInfo tableInfo = queriedTable.tableRelation().tableInfo();
            return isSysSchema(tableInfo.ident().schema()) || isEmptyRowTable(tableInfo);
        }
    }
}
