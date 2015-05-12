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

package io.crate.operation.reference.information;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.information.InformationTableConstraintsTableInfo;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.util.BytesRef;

import java.util.List;

public abstract class InformationTableConstraintsExpression<T> extends RowContextCollectorExpression<TableInfo, T> {

    private static final BytesRef PRIMARY_KEY = new BytesRef("PRIMARY_KEY");

    public static final TableConstraintsSchemaNameExpression SCHEMA_NAME_EXPRESSION = new TableConstraintsSchemaNameExpression();
    public static final TableConstraintsTableNameExpression TABLE_NAME_EXPRESSION = new TableConstraintsTableNameExpression();
    public static final TableConstraintsConstraintNameExpression CONSTRAINT_NAME_EXPRESSION = new TableConstraintsConstraintNameExpression();
    public static final TableConstraintsConstraintTypeExpression CONSTRAINT_TYPE_EXPRESSION = new TableConstraintsConstraintTypeExpression();

    protected InformationTableConstraintsExpression(ReferenceInfo info) {
        super(info);
    }

    public static class TableConstraintsSchemaNameExpression
            extends InformationTableConstraintsExpression<BytesRef> {

        protected TableConstraintsSchemaNameExpression() {
            super(InformationTableConstraintsTableInfo.ReferenceInfos.SCHEMA_NAME);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(row.schemaInfo().name());
        }
    }

    public static class TableConstraintsTableNameExpression
            extends InformationTableConstraintsExpression<BytesRef> {

        protected TableConstraintsTableNameExpression() {
            super(InformationTableConstraintsTableInfo.ReferenceInfos.TABLE_NAME);
        }

        @Override
        public BytesRef value() {
            assert row.ident().name() != null : "table name must not be null";
            return new BytesRef(row.ident().name());
        }
    }

    public static class TableConstraintsConstraintNameExpression
            extends InformationTableConstraintsExpression<BytesRef[]> {

        protected TableConstraintsConstraintNameExpression() {
            super(InformationTableConstraintsTableInfo.ReferenceInfos.CONSTRAINT_NAME);
        }

        @Override
        public BytesRef[] value() {
            BytesRef[] values = new BytesRef[row.primaryKey().size()];
            List<ColumnIdent> primaryKey = row.primaryKey();
            for (int i = 0, primaryKeySize = primaryKey.size(); i < primaryKeySize; i++) {
                values[i] = new BytesRef(primaryKey.get(i).fqn());
            }
            return values;
        }
    }

    public static class TableConstraintsConstraintTypeExpression
            extends InformationTableConstraintsExpression<BytesRef> {

        protected TableConstraintsConstraintTypeExpression() {
            super(InformationTableConstraintsTableInfo.ReferenceInfos.CONSTRAINT_TYPE);
        }

        @Override
        public BytesRef value() {
            return PRIMARY_KEY;
        }
    }
}
