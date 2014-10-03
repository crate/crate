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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.information.RowCollectExpression;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.util.BytesRef;


public abstract class InformationTablesExpression<T>
        extends RowCollectExpression<TableInfo, T> {

    public static final ImmutableList<InformationTablesExpression<?>> IMPLEMENTATIONS
            = ImmutableList.<InformationTablesExpression<?>>builder()
            .add(new InformationTablesExpression<BytesRef>("schema_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(Objects.firstNonNull(row.ident().schema(),
                            DocSchemaInfo.NAME));
                }
            })
            .add(new InformationTablesExpression<BytesRef>("table_name") {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.ident().name());
                }
            })
            .add(new InformationTablesExpression<BytesRef>("clustered_by") {
                @Override
                public BytesRef value() {
                    if (row.clusteredBy() != null) {
                        return new BytesRef(row.clusteredBy().fqn());
                    } else {
                        return null;
                    }
                }
            })
            .add(new InformationTablesExpression<BytesRef[]>("partitioned_by") {
                @Override
                public BytesRef[] value() {
                    int numPartitionedByCols = row.partitionedBy().size();
                    if (row.partitionedBy() != null && numPartitionedByCols > 0) {
                        BytesRef[] partitions = new BytesRef[numPartitionedByCols];
                        for (int i = 0; i < numPartitionedByCols; i++) {
                            partitions[i] = new BytesRef(row.partitionedBy().get(i).fqn());
                        }
                        return partitions;
                    } else {
                        return null;
                    }
                }
            })
            .add(new InformationTablesExpression<Integer>("number_of_shards") {
                @Override
                public Integer value() {
                    return row.numberOfShards();
                }
            })
            .add(new InformationTablesExpression<BytesRef>("number_of_replicas") {
                @Override
                public BytesRef value() {
                    return row.numberOfReplicas();
                }
            })
            .add(new InformationTablesExpression<BytesRef>("blobs_path") {
                @Override
                public BytesRef value() {
                    if (row instanceof BlobTableInfo) {
                        return ((BlobTableInfo)row).blobsPath();
                    }
                    return null;
                }
            })
            .build();

    protected InformationTablesExpression(String name) {
        super(InformationSchemaInfo.TABLE_INFO_TABLES.getColumnInfo(new ColumnIdent(name)));
    }

}
