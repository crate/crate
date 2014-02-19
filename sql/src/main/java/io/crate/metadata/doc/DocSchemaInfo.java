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

package io.crate.metadata.doc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.inject.Inject;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

public class DocSchemaInfo implements SchemaInfo, ClusterStateListener {

    public static final String NAME = "doc";
    private final ClusterService clusterService;

    private final LoadingCache<String, DocTableInfo> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(
                    new CacheLoader<String, DocTableInfo>() {
                        @Override
                        public DocTableInfo load(String key) throws Exception {
                            return innerGetTableInfo(key);
                        }
                    }
            );

    @Inject
    public DocSchemaInfo(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.add(this);
    }

    private DocTableInfo innerGetTableInfo(String name) {
        boolean checkAliasSchema = clusterService.state().metaData().settings().getAsBoolean("crate.table_alias.schema_check", true);
        DocTableInfoBuilder builder = new DocTableInfoBuilder(
                new TableIdent(NAME, name), clusterService, checkAliasSchema);
        return builder.build();
    }

    @Override
    public DocTableInfo getTableInfo(String name) {
        // TODO: implement index based tables
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new CrateException("Failed to get TableInfo", e.getCause());
        }
    }

    @Override
    public Collection<String> tableNames() {
        return Arrays.asList(clusterService.state().metaData().concreteAllIndices());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged()) {
            cache.invalidateAll();
        }
    }

}