/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.shard;

import com.google.common.collect.ImmutableMap;
import io.crate.PartitionName;
import io.crate.exceptions.CrateException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoBuilder;
import io.crate.operation.reference.partitioned.PartitionedColumnExpression;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class ShardReferenceResolver extends AbstractReferenceResolver {

    private final Map<ReferenceIdent, ReferenceImplementation> implementations;

    @Inject
    public ShardReferenceResolver(Index index,
                                  ClusterService clusterService,
                                  final Map<ReferenceIdent, ReferenceImplementation> globalImplementations,
                                  final Map<ReferenceIdent, ShardReferenceImplementation> shardImplementations) {
        ImmutableMap.Builder<ReferenceIdent, ReferenceImplementation> builder = ImmutableMap.builder();
                builder.putAll(globalImplementations)
                .putAll(shardImplementations);
        String realTableName = null;
        try {
            realTableName = PartitionName.tableName(index.name());
        } catch (IllegalArgumentException e) {
            // no partition - ignore
        }
        if (realTableName != null) {
            // get DocTableInfo for virtual partitioned table
            DocTableInfo info = new DocTableInfoBuilder(
                    new TableIdent(DocSchemaInfo.NAME, realTableName),
                    clusterService, true).build();
            assert info.isPartitioned();
            int i = 0;
            int numPartitionedColumns = info.partitionedByColumns().size();

            PartitionName partitionName;
            try {
                partitionName = PartitionName.fromString(
                        index.name(),
                        realTableName,
                        numPartitionedColumns);
            } catch (IOException e) {
                throw new CrateException(
                        String.format(Locale.ENGLISH,
                                "Unable to load PARTITIONED BY columns from partition %s",
                                index.name())
                );
            }
            assert partitionName.values().size() == numPartitionedColumns : "invalid number of partitioned columns";
            for (ReferenceInfo partitionedInfo : info.partitionedByColumns()) {
                builder.put(partitionedInfo.ident(), new PartitionedColumnExpression(
                        partitionedInfo,
                        partitionName.values().get(i)
                ));
                i++;
            }
        }
        this.implementations = builder.build();

    }

    @Override
    protected Map<ReferenceIdent, ReferenceImplementation> implementations() {
        return implementations;
    }

}
