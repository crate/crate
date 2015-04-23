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

package io.crate.operation.reference.sys.node;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.reference.sys.node.fs.NodeFsExpression;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

public class SysNodeExpressionModule extends AbstractModule {

    private Map<ColumnIdent, ReferenceInfo> infos;
    private MapBinder<ReferenceIdent, ReferenceImplementation> refBinder;

    private void bindExpr(String name, Class clazz) {
        refBinder.addBinding(infos.get(new ColumnIdent(name)).ident()).to(clazz).asEagerSingleton();
    }

    @Override
    protected void configure() {
        refBinder = MapBinder.newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
        infos = SysNodesTableInfo.INFOS;
        bind(NodeSysExpression.class).asEagerSingleton();

        bindExpr(NodeFsExpression.NAME, NodeFsExpression.class);
        bindExpr(NodeHostnameExpression.NAME, NodeHostnameExpression.class);
        bindExpr(NodeRestUrlExpression.NAME, NodeRestUrlExpression.class);
        bindExpr(NodeIdExpression.NAME, NodeIdExpression.class);
        bindExpr(NodeLoadExpression.NAME, NodeLoadExpression.class);
        bindExpr(NodeMemoryExpression.NAME, NodeMemoryExpression.class);
        bindExpr(NodeNameExpression.NAME, NodeNameExpression.class);
        bindExpr(NodePortExpression.NAME, NodePortExpression.class);
        bindExpr(NodeHeapExpression.NAME, NodeHeapExpression.class);
        bindExpr(NodeVersionExpression.NAME, NodeVersionExpression.class);
        bindExpr(NodeThreadPoolsExpression.NAME, NodeThreadPoolsExpression.class);
        bindExpr(NodeNetworkExpression.NAME, NodeNetworkExpression.class);
        bindExpr(NodeOsExpression.NAME, NodeOsExpression.class);
        bindExpr(NodeProcessExpression.NAME, NodeProcessExpression.class);
    }
}
