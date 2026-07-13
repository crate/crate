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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.expression.reference.sys.check.AbstractSysCheck;
import io.crate.metadata.RelationName;

@Singleton
public class TablesNeedUpgradeSysCheck extends AbstractSysCheck {

    public static final int ID = 3;
    public static final String DESCRIPTION =
        "The following tables need to be recreated for compatibility with future major versions of CrateDB: ";

    private final ClusterService clusterService;
    private volatile Collection<RelationName> tablesNeedUpgrade;

    @Inject
    public TablesNeedUpgradeSysCheck(ClusterService clusterService) {
        super(ID, DESCRIPTION, Severity.LOW);
        this.clusterService = clusterService;
    }

    @Override
    public String description() {
        return DESCRIPTION + tablesNeedUpgrade + ' ' + CLUSTER_CHECK_LINK_PATTERN + ID;
    }

    @Override
    public CompletableFuture<?> computeResult() {
        ArrayList<RelationName> tables = new ArrayList<>();
        Metadata metadata = clusterService.state().metadata();

        table_loop:
        for (var table : metadata.relations(RelationMetadata.Table.class)) {
            for (String indexUUID : table.indexUUIDs()) {
                IndexMetadata index = metadata.index(indexUUID);
                if (index == null) {
                    continue;
                }
                if (index.getCreationVersion().major < Version.CURRENT.major) {
                    tables.add(table.name());
                    continue table_loop;
                }
            }
        }
        tablesNeedUpgrade = tables;
        return CompletableFuture.completedFuture(tables);
    }

    @Override
    public boolean isValid() {
        return tablesNeedUpgrade == null || tablesNeedUpgrade.isEmpty();
    }
}
