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

package io.crate.metadata;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;

import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;

public class PartitionInfos implements Iterable<PartitionInfo> {

    private final ClusterService clusterService;
    private final Schemas schemas;

    public PartitionInfos(ClusterService clusterService, Schemas schemas) {
        this.clusterService = clusterService;
        this.schemas = schemas;
    }

    @Override
    public Iterator<PartitionInfo> iterator() {
        Metadata metadata = clusterService.state().metadata();
        return StreamSupport.stream(schemas.spliterator(), false)
            .filter(schemaInfo -> schemaInfo instanceof DocSchemaInfo)
            .flatMap(docSchemaInfo -> StreamSupport.stream(docSchemaInfo.getTables().spliterator(), false))
            .flatMap(docTable -> ((DocTableInfo) docTable).getPartitions(metadata).stream())
            .iterator();
    }
}
