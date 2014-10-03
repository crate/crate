/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TablePartitionInfo;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.information.RowCollectExpression;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public abstract class InformationTablePartitionsExpression<T>
        extends RowCollectExpression<TablePartitionInfo, T> {

    public static final ImmutableList<InformationTablePartitionsExpression<?>> IMPLEMENTATIONS
            = ImmutableList.<InformationTablePartitionsExpression<?>>builder()
            .add(new InformationTablePartitionsExpression<BytesRef>("table_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.tableName());
                }
            })
            .add(new InformationTablePartitionsExpression<BytesRef>("schema_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.schemaName());
                }
            })
            .add(new InformationTablePartitionsExpression<BytesRef>("partition_ident") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.partitionIdent());
                }
            })
            .add(new InformationTablePartitionsExpression<Map<String, Object>>("values") {
                @Override
                public Map<String, Object> value() {
                    return row.values();
                }
            })
            .build();

    protected InformationTablePartitionsExpression(String name) {
        super(InformationSchemaInfo.TABLE_INFO_TABLE_PARTITIONS.getColumnInfo(new ColumnIdent(name)));
    }
}
