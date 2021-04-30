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

package io.crate.execution.dsl.projection;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public abstract class Projection {

    static final Predicate<Projection> IS_SHARD_PROJECTION = p -> p.requiredGranularity() == RowGranularity.SHARD;
    static final Predicate<Projection> IS_NODE_PROJECTION = IS_SHARD_PROJECTION.negate();

    /**
     * The granularity required to run this projection
     * <p>
     * For example:
     * <p>
     * CLUSTER - projection may run in any context on the cluster and receive any rows.
     * <p>
     * SHARD - projection must be run in a shard-context and it must only receive rows from a
     * single shard.
     */
    public RowGranularity requiredGranularity() {
        return RowGranularity.CLUSTER;
    }

    public interface ProjectionFactory<T extends Projection> {
        T newInstance(StreamInput in) throws IOException;
    }

    public abstract ProjectionType projectionType();

    public abstract <C, R> R accept(ProjectionVisitor<C, R> visitor, C context);

    public abstract List<? extends Symbol> outputs();

    public static void toStream(Projection projection, StreamOutput out) throws IOException {
        out.writeVInt(projection.projectionType().ordinal());
        projection.writeTo(out);
    }

    public static Projection fromStream(StreamInput in) throws IOException {
        return ProjectionType.values()[in.readVInt()].newInstance(in);
    }

    public abstract void writeTo(StreamOutput out) throws IOException;

    // force subclasses to implement equality
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public int hashCode() {
        return projectionType().hashCode();
    }

    public Map<String, Object> mapRepresentation() {
        return Map.of("type", projectionType().toString());
    }
}
