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

package io.crate.planner.projection;

import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.List;

public abstract class Projection implements Streamable {

    /**
     * The granularity required to run this projection
     *
     * For example:
     *
     *  CLUSTER - projection may run in any context on the cluster and receive any rows.
     *
     *  SHARD - projection must be run in a shard-context and it must only receive rows from a
     *  single shard.
     */
    public RowGranularity requiredGranularity() {
        return RowGranularity.CLUSTER;
    }

    public interface ProjectionFactory<T extends Projection> {
        T newInstance();
    }

    public abstract ProjectionType projectionType();

    public abstract <C, R> R accept(ProjectionVisitor<C, R> visitor, C context);

    public abstract List<? extends Symbol> outputs();

    public static void toStream(Projection projection, StreamOutput out) throws IOException {
        out.writeVInt(projection.projectionType().ordinal());
        projection.writeTo(out);
    }

    public static Projection fromStream(StreamInput in) throws IOException {
        Projection projection = ProjectionType.values()[in.readVInt()].newInstance();
        projection.readFrom(in);

        return projection;
    }

    // force subclasses to implement equality
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public int hashCode() {
        return projectionType().hashCode();
    }
}
