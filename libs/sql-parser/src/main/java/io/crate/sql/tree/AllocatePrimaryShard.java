/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.sql.tree;

import java.util.Objects;
import java.util.function.Function;

public class AllocatePrimaryShard<T> extends RerouteOption {

    public static class Properties {
        public static final String ACCEPT_DATA_LOSS = "accept_data_loss";
    }

    public enum Type {
        EMPTY,
        STALE;
    }

    private final T node;
    private final T shardId;
    private final Type type;
    private final GenericProperties<T> properties;

    public AllocatePrimaryShard(T node, T shardId, Type type, GenericProperties<T> properties) {
        this.node = node;
        this.shardId = shardId;
        this.type = type;
        this.properties = properties;
    }

    public T node() {
        return node;
    }

    public T shardId() {
        return shardId;
    }

    public Type type() {
        return type;
    }

    public GenericProperties<T> properties() {
        return properties;
    }

    public <U> AllocatePrimaryShard<U> map(Function<? super T, ? extends U> mapper) {
        return new AllocatePrimaryShard<>(
            mapper.apply(node),
            mapper.apply(shardId),
            type,
            properties.map(mapper));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllocatePrimaryShard<?> that = (AllocatePrimaryShard<?>) o;
        return Objects.equals(node, that.node) &&
            Objects.equals(shardId, that.shardId) &&
            type == that.type &&
            Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, shardId, type, properties);
    }

    @Override
    public String toString() {
        return "AllocatePrimaryShard{" +
            "node=" + node +
            ", shardId=" + shardId +
            ", type=" + type +
            ", properties=" + properties +
            '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRerouteAllocatePrimaryShard(this, context);
    }
}
