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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import io.crate.expression.reference.sys.check.AbstractSysCheck;
import io.crate.metadata.RelationName;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TablesNeedUpgradeSysCheck extends AbstractSysCheck {

    public static final int ID = 3;
    public static final String DESCRIPTION =
        "The following tables need to be recreated for compatibility with future major versions of CrateDB: ";

    private final ClusterService clusterService;
    private volatile Collection<String> tablesNeedUpgrade;

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
        HashSet<String> fqTables = new HashSet<>();
        for (ObjectCursor<IndexMetadata> cursor : clusterService.state()
            .metadata()
            .indices()
            .values()) {

            if (cursor.value.getCreationVersion().major < org.elasticsearch.Version.CURRENT.major) {
                fqTables.add(RelationName.fqnFromIndexName(cursor.value.getIndex().getName()));
            }
        }
        tablesNeedUpgrade = fqTables;
        return CompletableFuture.completedFuture(fqTables);
    }

    @Override
    public boolean isValid() {
        return tablesNeedUpgrade == null || tablesNeedUpgrade.isEmpty();
    }
}
