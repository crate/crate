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

package io.crate.operator.reference.information;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.InformationCollectorExpression;
import io.crate.metadata.information.InformationSchemaInfo;
import org.apache.lucene.util.BytesRef;


public abstract class InformationColumnsExpression<T>
        extends InformationCollectorExpression<ColumnContext, T> {

    public static final ImmutableList<InformationColumnsExpression<?>> IMPLEMENTATIONS
            = ImmutableList.<InformationColumnsExpression<?>>builder()
            .add(new InformationColumnsExpression<BytesRef>("schema_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(Objects.firstNonNull(row.info.ident().tableIdent().schema(),
                            DocSchemaInfo.NAME));
                }
            })
            .add(new InformationColumnsExpression<BytesRef>("table_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.info.ident().tableIdent().name());
                }
            })
            .add(new InformationColumnsExpression<BytesRef>("column_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.info.ident().columnIdent().fqn());
                }
            })
            .add(new InformationColumnsExpression<Short>("ordinal_position") {
                @Override
                public Short value() {
                    return row.ordinal;
                }
            })
            .add(new InformationColumnsExpression<BytesRef>("data_type") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.info.type().getName());
                }
            })
            .build();

    protected InformationColumnsExpression(String name) {
        super(InformationSchemaInfo.TABLE_INFO_COLUMNS.getColumnInfo(new ColumnIdent(name)));
    }

}
