/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.reference.sys.check.node;

import io.crate.data.Input;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysNodeChecksTableInfo;
import io.crate.operation.reference.sys.SysRowUpdater;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.discovery.Discovery;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

@Singleton
public class SysNodeChecks implements SysRowUpdater<SysNodeCheck>, Iterable<SysNodeCheck> {

    private final Map<BytesRef, SysNodeCheck> checks;
    private static final BiConsumer<SysNodeCheck, Input<?>> ackWriter =
        (row, input) -> row.acknowledged((Boolean) input.value());

    @Inject
    public SysNodeChecks(Map<Integer, SysNodeCheck> checks, Discovery discovery, ClusterService clusterService) {
        this.checks = new HashMap<>(checks.size());
        // we need to wait for the discovery to finish to have a local node id
        discovery.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                BytesRef nodeId = new BytesRef(clusterService.localNode().getId());
                for (SysNodeCheck sysNodeCheck : checks.values()) {
                    sysNodeCheck.setNodeId(nodeId);
                    SysNodeChecks.this.checks.put(sysNodeCheck.rowId(), sysNodeCheck);
                }
            }
        });
    }

    @Override
    public SysNodeCheck getRow(Object id) {
        assert id instanceof BytesRef: "an integer is required as id";
        return checks.get(id);
    }

    @Override
    public BiConsumer<SysNodeCheck, Input<?>> getWriter(ColumnIdent ci) {
        if (SysNodeChecksTableInfo.Columns.ACKNOWLEDGED.equals(ci)){
            return ackWriter;
        }
        return null;
    }

    @Override
    public Iterator<SysNodeCheck> iterator() {
        return checks.values().iterator();
    }
}
