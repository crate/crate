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

package org.cratedb.action.collect.scope;

public enum ExpressionScope {

    /**
     * Expressions with Cluster Scope depend only on the cluster and its state.
     * They can be evaluated anywhere in the cluster.
     */
    CLUSTER("cluster"),

    /**
     * Expressions with Node Scope depend on the node they are evaluated on.
     * They yield different values for different nodes.
     */
    NODE("node"),

    /**
     * Table scoped Expressions depend are scoped to a single table (index) and
     * can only be executed on nodes that have shards of that table (index)
     * or on any shard of that table (index).
     */
    TABLE("table"),

    /**
     * Expressions scoped to a single shard must be executed on shard-level.
     * E.g. getting a field value from an index is scoped to shard-level.
     */
    SHARD("shard");

    private String tableName;

    private ExpressionScope(String tableName) {
        this.tableName = tableName;
    }

    public String tableName() {
        return tableName;
    }
}
