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

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

public class RerouteCancelShard extends RerouteOption {

    private final Expression nodeId;
    private final Expression shardId;
    @Nullable
    private final GenericProperties properties;

    public RerouteCancelShard(Expression shardId, Expression nodeId, @Nullable GenericProperties properties) {
        this.shardId = shardId;
        this.nodeId = nodeId;
        this.properties = properties;
    }

    public Expression nodeId() {
        return nodeId;
    }

    public Expression shardId() {
        return shardId;
    }

    @Nullable
    public GenericProperties properties() {
        return properties;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(shardId, nodeId, properties);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        RerouteCancelShard that = (RerouteCancelShard) obj;

        if (!shardId.equals(that.shardId)) return false;
        if (!nodeId.equals(that.nodeId)) return false;
        if (!properties.equals(that.properties)) return false;

        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("shardId", shardId)
            .add("nodeId", nodeId)
            .add("properties", properties).toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRerouteCancelShard(this, context);
    }
}
