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

package io.crate.operator.reference.sys.cluster;

import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysExpression;
import org.cratedb.DataType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;

public class ClusterNameExpression extends SysExpression<String> {

    public static final String COLNAME = "name";
    public static final ReferenceInfo INFO_NAME = SysClusterTableInfo.register(COLNAME, DataType.STRING, null);


    private final ClusterName clusterName;

    @Inject
    public ClusterNameExpression(ClusterName clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String value() {
        return clusterName.value();
    }

    @Override
    public ReferenceInfo info() {
        return INFO_NAME;
    }

}
