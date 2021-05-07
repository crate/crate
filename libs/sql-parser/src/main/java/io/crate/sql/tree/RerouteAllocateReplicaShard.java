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

package io.crate.sql.tree;

import java.util.Objects;
import java.util.function.Function;

public class RerouteAllocateReplicaShard<T> extends RerouteOption {

    private final T shardId;
    private final T nodeIdOrName;

    public RerouteAllocateReplicaShard(T shardId, T nodeIdOrName) {
        this.shardId = shardId;
        this.nodeIdOrName = nodeIdOrName;
    }

    public T nodeIdOrName() {
        return nodeIdOrName;
    }

    public T shardId() {
        return shardId;
    }

    public <U> RerouteAllocateReplicaShard<U> map(Function<? super T, ? extends U> mapper) {
        return new RerouteAllocateReplicaShard<>(
            mapper.apply(shardId),
            mapper.apply(nodeIdOrName));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RerouteAllocateReplicaShard<?> that = (RerouteAllocateReplicaShard<?>) o;
        return Objects.equals(nodeIdOrName, that.nodeIdOrName) &&
               Objects.equals(shardId, that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeIdOrName, shardId);
    }

    @Override
    public String toString() {
        return "RerouteAllocateReplicaShard{" +
               "shardId=" + shardId +
               ", nodeId=" + nodeIdOrName +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRerouteAllocateReplicaShard(this, context);
    }
}
