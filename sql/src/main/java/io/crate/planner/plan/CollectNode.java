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

package io.crate.planner.plan;

import com.google.common.base.Optional;
import io.crate.metadata.Routing;
import io.crate.planner.symbol.Function;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;

@Deprecated
public class CollectNode extends TopNNode {

    private Routing routing;
    private Optional<Function> whereClause;

    public CollectNode() {
        super();
    }

    public CollectNode(String id, Routing routing) {
        this(id, routing, null);
    }

    public CollectNode(String id, Routing routing, @Nullable Function whereClause) {
        super(id, NO_LIMIT, NO_OFFSET, new int[0], new boolean[0]);
        this.routing = routing;
        this.whereClause = Optional.fromNullable(whereClause);
    }

    public CollectNode(String id, Routing routing, @Nullable Function whereClause,
                       int limit, int offset) {
        super(id, limit, offset, new int[0], new boolean[0]);
        this.routing = routing;
        this.whereClause = Optional.fromNullable(whereClause);
    }

    public CollectNode(String id, Routing routing, @Nullable Function whereClause,
                       int limit, int offset, int[] orderByIndices, boolean[] reverseFlags) {
        super(id, limit, offset, orderByIndices, reverseFlags);
        this.routing = routing;
        this.whereClause = Optional.fromNullable(whereClause);
    }

    public Routing routing() {
        return routing;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }

    public Optional<Function> whereClause() {
        return whereClause;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollect(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            routing = new Routing();
            routing.readFrom(in);
        }
        if (in.readBoolean()) {
            Function f = new Function();
            f.readFrom(in);
            whereClause = Optional.of(f);
        } else {
            whereClause = Optional.absent();
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (whereClause.isPresent()) {
            out.writeBoolean(true);
            whereClause.get().writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public void routing(Routing routing) {
        this.routing = routing;
    }
}
