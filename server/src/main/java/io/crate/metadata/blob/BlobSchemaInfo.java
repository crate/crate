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

package io.crate.metadata.blob;

import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class BlobSchemaInfo implements SchemaInfo {

    public static final String NAME = "blob";

    private final ClusterService clusterService;
    private final BlobTableInfoFactory blobTableInfoFactory;
    private final ConcurrentHashMap<String, BlobTableInfo> tableByName = new ConcurrentHashMap<>();

    @Inject
    public BlobSchemaInfo(ClusterService clusterService,
                          BlobTableInfoFactory blobTableInfoFactory) {
        this.clusterService = clusterService;
        this.blobTableInfoFactory = blobTableInfoFactory;
    }

    @Override
    public TableInfo getTableInfo(String name) {
        try {
            return tableByName.computeIfAbsent(
                name,
                n -> blobTableInfoFactory.create(new RelationName(NAME, n), clusterService.state())
            );
        } catch (Exception e) {
            if (e instanceof ResourceUnknownException) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void invalidateTableCache(String tableName) {
        tableByName.remove(tableName);
    }

    @Override
    public void update(ClusterChangedEvent event) {
        if (event.metadataChanged()) {
            tableByName.clear();
        }
    }

    @Override
    public Iterable<TableInfo> getTables() {
        return Stream.of(clusterService.state().metadata().getConcreteAllOpenIndices())
            .filter(BlobIndex::isBlobIndex)
            .map(BlobIndex::stripPrefix)
            .map(this::getTableInfo)
            ::iterator;
    }

    @Override
    public Iterable<ViewInfo> getViews() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
    }
}
