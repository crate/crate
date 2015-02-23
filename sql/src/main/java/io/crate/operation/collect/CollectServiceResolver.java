/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import io.crate.metadata.Routing;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import io.crate.metadata.sys.SysJobsTableInfo;
import io.crate.metadata.sys.SysOperationsLogTableInfo;
import io.crate.metadata.sys.SysOperationsTableInfo;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.DiscoveryService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectServiceResolver {

    private final Map<String, CollectService> services = new HashMap<>();
    private final DiscoveryService discoveryService;

    @Inject
    public CollectServiceResolver(DiscoveryService discoveryService,
                                  SystemCollectService systemCollectService) {
        this.discoveryService = discoveryService;

        services.put(SysJobsTableInfo.IDENT.fqn(), systemCollectService);
        services.put(SysJobsLogTableInfo.IDENT.fqn(), systemCollectService);
        services.put(SysOperationsTableInfo.IDENT.fqn(), systemCollectService);
        services.put(SysOperationsLogTableInfo.IDENT.fqn(), systemCollectService);
    }

    public CollectService getService(Routing routing) {
        Map<String, List<Integer>> tables = routing.locations().get(discoveryService.localNode().id());
        if (tables == null) return null;
        if (tables.size() == 0) return null;
        assert tables.size() == 1;
        return services.get(tables.keySet().iterator().next());
    }
}
