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

package io.crate.operation.reference.sys.cluster;

import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.sys.SysClusterTableInfo;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SysClusterExpressionModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ClusterReferenceResolver.class).asEagerSingleton();
        MapBinder<ReferenceIdent, ReferenceImplementation> b = MapBinder
            .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);

        b.addBinding(clusterIdent(ClusterIdExpression.NAME)).to(ClusterIdExpression.class).asEagerSingleton();
        b.addBinding(clusterIdent(ClusterNameExpression.NAME)).to(ClusterNameExpression.class).asEagerSingleton();
        b.addBinding(clusterIdent(ClusterMasterNodeExpression.NAME)).to(ClusterMasterNodeExpression.class).asEagerSingleton();
        b.addBinding(clusterIdent(ClusterSettingsExpression.NAME)).to(ClusterSettingsExpression.class).asEagerSingleton();
    }

    private ReferenceIdent clusterIdent(String name) {
        return new ReferenceIdent(SysClusterTableInfo.IDENT, name);
    }
}
