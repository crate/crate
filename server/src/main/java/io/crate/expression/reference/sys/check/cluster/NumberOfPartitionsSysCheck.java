/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.reference.sys.check.cluster;

import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.expression.reference.sys.check.AbstractSysCheck;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class NumberOfPartitionsSysCheck extends AbstractSysCheck {

    private static final int ID = 2;
    private static final String DESCRIPTION =
        "The total number of partitions of one or more partitioned tables should" +
        " not be greater than 1000. A large amount of shards can significantly reduce performance.";

    private static final int PARTITIONS_THRESHOLD = 1000;
    private final Schemas schemas;
    private final ClusterService clusterService;

    @Inject
    public NumberOfPartitionsSysCheck(NodeContext nodeContext, ClusterService clusterService) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.schemas = nodeContext.schemas();
        this.clusterService = clusterService;
    }

    @Override
    public boolean isValid() {
        Metadata metadata = clusterService.state().metadata();
        for (SchemaInfo schemaInfo : schemas) {
            if (!(schemaInfo instanceof DocSchemaInfo docSchemaInfo)) {
                continue;
            }
            for (var table : docSchemaInfo.getTables()) {
                DocTableInfo docTableInfo = (DocTableInfo) table;
                if (docTableInfo.isPartitioned() && docTableInfo.getPartitions(metadata).size() > PARTITIONS_THRESHOLD) {
                    return false;
                }
            }
        }
        return true;
    }
}
