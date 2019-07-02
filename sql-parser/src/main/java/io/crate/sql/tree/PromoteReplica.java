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

package io.crate.sql.tree;

public class PromoteReplica extends RerouteOption {

    private Expression node;
    private final Expression shardId;
    private final GenericProperties properties;

    public static class Properties {
        public static final String ACCEPT_DATA_LOSS = "accept_data_loss";
    }

    public PromoteReplica(Expression node, Expression shardId, GenericProperties properties) {
        this.node = node;
        this.shardId = shardId;
        this.properties = properties;
    }

    public Expression node() {
        return node;
    }

    public Expression shardId() {
        return shardId;
    }

    public GenericProperties properties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitReroutePromoteReplica(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PromoteReplica that = (PromoteReplica) o;

        if (!node.equals(that.node)) {
            return false;
        }
        if (!shardId.equals(that.shardId)) {
            return false;
        }
        return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        int result = node.hashCode();
        result = 31 * result + shardId.hashCode();
        result = 31 * result + properties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PromoteReplica{" +
               "node=" + node +
               ", shardId=" + shardId +
               ", properties=" + properties +
               '}';
    }
}
