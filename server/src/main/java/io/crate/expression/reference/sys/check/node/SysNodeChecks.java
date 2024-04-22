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

package io.crate.expression.reference.sys.check.node;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.data.Input;
import io.crate.expression.reference.sys.SysRowUpdater;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysNodeChecksTableInfo;

@Singleton
public class SysNodeChecks implements SysRowUpdater<SysNodeCheck>, Iterable<SysNodeCheck> {

    private final Map<String, SysNodeCheck> checks;
    private static final BiConsumer<SysNodeCheck, Input<?>> ACK_WRITER =
        (row, input) -> row.acknowledged((Boolean) input.value());

    @Inject
    public SysNodeChecks(Map<Integer, SysNodeCheck> checks, Coordinator coordinator, ClusterService clusterService) {
        this.checks = new HashMap<>(checks.size());
        // we need to wait for the discovery to finish to have a local node id
        coordinator.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                String nodeId = clusterService.localNode().getId();
                for (SysNodeCheck sysNodeCheck : checks.values()) {
                    sysNodeCheck.setNodeId(nodeId);
                    SysNodeChecks.this.checks.put(sysNodeCheck.rowId(), sysNodeCheck);
                }
            }
        });
    }

    @Override
    public SysNodeCheck getRow(Object id) {
        return checks.get(id);
    }

    @Override
    public BiConsumer<SysNodeCheck, Input<?>> getWriter(ColumnIdent ci) {
        if (SysNodeChecksTableInfo.Columns.ACKNOWLEDGED.equals(ci)) {
            return ACK_WRITER;
        }
        return null;
    }

    @Override
    public Iterator<SysNodeCheck> iterator() {
        return checks.values().iterator();
    }
}
