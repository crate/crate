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

package io.crate.metadata.sys;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class provides a static registry of global system table metadata.
 */
public class SystemReferences {

    public static final String SCHEMA = "sys";
    public static final TableIdent CLUSTER_IDENT = new TableIdent(SCHEMA, "cluster");
    public static final TableIdent NODES_IDENT = new TableIdent(SCHEMA, "nodes");

    private static final Map<ReferenceIdent, ReferenceInfo> referenceInfos = new ConcurrentHashMap<>();

    public static ReferenceInfo register(ReferenceInfo info) {
        referenceInfos.put(info.ident(), info);
        return info;
    }

    public static ReferenceInfo get(ReferenceIdent ident) {
        return referenceInfos.get(ident);
    }

    public static ReferenceInfo registerNodeReference(String column, DataType type, List<String> path) {
        return register(new ReferenceInfo(
                new ReferenceIdent(NODES_IDENT, column, path),
                RowGranularity.NODE,
                type));
    }

    public static ReferenceInfo registerNodeReference(String column, DataType type) {
        return register(new ReferenceInfo(
                new ReferenceIdent(NODES_IDENT, column),
                RowGranularity.NODE,
                type));
    }

    public static ReferenceInfo registerClusterReference(String column, DataType type) {
        return register(new ReferenceInfo(
                new ReferenceIdent(CLUSTER_IDENT, column),
                RowGranularity.CLUSTER,
                type));
    }

}