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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.information.InformationCollectorExpression;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.util.BytesRef;

import java.util.Set;

public abstract class InformationTableConstraintsExpression<T> extends InformationCollectorExpression<TableInfo, T> {
    private static final BytesRef PRIMARY_KEY = new BytesRef("PRIMARY_KEY");
    public static final ImmutableList<InformationTableConstraintsExpression<?>> IMPLEMENTATIONS =
            ImmutableList.<InformationTableConstraintsExpression<?>>builder()
                    .add(new InformationTableConstraintsExpression<BytesRef>("schema_name") {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(row.ident().schema());
                        }
                    })
                    .add(new InformationTableConstraintsExpression<BytesRef>("table_name") {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(row.ident().name());
                        }
                    })
                    .add(new InformationTableConstraintsExpression<Set<BytesRef>>("constraint_name") {
                        @Override
                        public Set<BytesRef> value() {
                            ImmutableSet.Builder<BytesRef> builder = ImmutableSet.builder();
                            for (String primaryKeyColumn : row.primaryKey()) {
                                builder.add(new BytesRef(primaryKeyColumn));
                            }
                            return builder.build();
                        }
                    })
                    .add(new InformationTableConstraintsExpression<BytesRef>("constraint_type") {
                        @Override
                        public BytesRef value() {
                            return PRIMARY_KEY;
                        }
                    })
                    .build();

    protected InformationTableConstraintsExpression(String name) {
        super(InformationSchemaInfo.TABLE_INFO_TABLE_CONSTRAINTS.getColumnInfo(new ColumnIdent(name)));
    }
}
