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

package io.crate.metadata.blob;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.blob.BlobEnvironment;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

public class BlobSchemaInfo implements SchemaInfo, ClusterStateListener {

    public static final String NAME = "blob";

    private final ClusterService clusterService;
    private final BlobEnvironment blobEnvironment;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Environment environment;
    private final Functions functions;

    private final LoadingCache<String, BlobTableInfo> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(
                    new CacheLoader<String, BlobTableInfo>() {
                        @Override
                        public BlobTableInfo load(String key) throws Exception {
                            return innerGetTableInfo(key);
                        }
                    }
            );

    private final Function<String, TableInfo> tableInfoFunction;

    @Inject
    public BlobSchemaInfo(ClusterService clusterService,
                          BlobEnvironment blobEnvironment,
                          IndexNameExpressionResolver indexNameExpressionResolver,
                          Environment environment,
                          Functions functions) {
        this.clusterService = clusterService;
        this.blobEnvironment = blobEnvironment;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.environment = environment;
        this.functions = functions;
        clusterService.add(this);
        tableInfoFunction = new Function<String, TableInfo>() {
            @Nullable
            @Override
            public TableInfo apply(@Nullable String input) {
                return getTableInfo(input);
            }
        };
    }

    private BlobTableInfo innerGetTableInfo(String name) {
        BlobTableInfoBuilder builder = new BlobTableInfoBuilder(
                new TableIdent(NAME, name), clusterService, indexNameExpressionResolver, blobEnvironment, environment, functions);
        return builder.build();
    }

    @Override
    public BlobTableInfo getTableInfo(String name) {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new UnhandledServerException("Failed to get TableInfo", e.getCause());
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TableUnknownException) {
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
    public boolean systemSchema() {
        return true;
    }

    @Override
    public void invalidateTableCache(String tableName) {
        cache.invalidate(tableName);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged()) {
            cache.invalidateAll();
        }
    }

    @Override
    public Iterator<TableInfo> iterator() {
        return tableNamesIterable().transform(tableInfoFunction).iterator();
    }

    private FluentIterable<String> tableNamesIterable() {
        // TODO: once we support closing/opening tables change this to concreteIndices()
        // and add  state info to the TableInfo.
        return FluentIterable
                .from(Arrays.asList(clusterService.state().metaData().concreteAllOpenIndices()))
                .filter(BlobIndices.indicesFilter)
                .transform(BlobIndices.STRIP_PREFIX);
    }

    @Override
    public void close() throws Exception {
        clusterService.remove(this);
    }
}
