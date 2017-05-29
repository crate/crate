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

import io.crate.metadata.RowContextCollectorExpression;
import org.apache.lucene.util.BytesRef;


public abstract class InformationColumnsExpression<T>
    extends RowContextCollectorExpression<ColumnContext, T> {

    public static class ColumnsSchemaNameExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().schema() != null : "table schema can't be null";
            return new BytesRef(row.info.ident().tableIdent().schema());
        }
    }

    public static class ColumnsTableNameExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().name() != null : "table name can't be null";
            return new BytesRef(row.info.ident().tableIdent().name());
        }
    }

    public static class ColumnsTableCatalogExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().schema() != null : "table schema can't be null";
            return new BytesRef(row.info.ident().tableIdent().name());
        }
    }

    public static class ColumnsColumnNameExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().name() != null : "column name name can't be null";
            return new BytesRef(row.info.ident().columnIdent().sqlFqn());
        }
    }

    public static class ColumnsNullExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            return null;
        }
    }

    public static class ColumnsOrdinalExpression extends InformationColumnsExpression<Short> {

        @Override
        public Short value() {
            return row.ordinal;
        }
    }

    public static class ColumnsDataTypeExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.valueType() != null && row.info.valueType().getName() !=
                                                   null : "columns must always have a type and the type must have a name";
            return new BytesRef(row.info.valueType().getName());
        }
    }

    public static class ColumnsIsNullableExpression extends InformationColumnsExpression<Boolean> {

        @Override
        public Boolean value() {
            if (row.tableInfo.primaryKey().contains(row.info.ident().columnIdent())) {
                return false;
            } else {
                return row.info.isNullable();
            }
        }
    }
}
