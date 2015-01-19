/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.action.sql.query;

import com.google.common.base.Optional;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryShardRequest extends ActionRequest<QueryShardRequest> {

    private String index;
    private Integer shard;
    private List<? extends Symbol> outputs;
    private List<Symbol> orderBy;
    private boolean[] reverseFlags;
    private Boolean[] nullsFirst;
    private int limit;
    private int offset;
    private WhereClause whereClause;
    private List<ReferenceInfo> partitionBy;

    // used for paged QTF queries
    private Optional<Scroll> scroll;

    public QueryShardRequest() {}

    public QueryShardRequest(String index,
                             int shard,
                             List<? extends Symbol> outputs,
                             List<Symbol> orderBy,
                             boolean[] reverseFlags,
                             Boolean[] nullsFirst,
                             int limit,
                             int offset,
                             WhereClause whereClause,
                             List<ReferenceInfo> partitionBy,
                             Optional<TimeValue> keepAlive
    ) {
        this.index = index;
        this.shard = shard;
        this.outputs = outputs;
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
        this.limit = limit;
        this.offset = offset;
        this.whereClause = whereClause;
        this.partitionBy = partitionBy;

        if (keepAlive.isPresent()) {
            this.scroll = Optional.of(new Scroll(keepAlive.get()));
        } else {
            this.scroll = Optional.absent();
        }
    }


    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        shard = in.readVInt();

        int numOutputs = in.readVInt();
        List<Symbol> outputs = new ArrayList<>(numOutputs);
        for (int i = 0; i < numOutputs; i++) {
            outputs.add(Symbol.fromStream(in));
        }
        this.outputs = outputs;

        int numOrderBy = in.readVInt();
        orderBy = new ArrayList<>(numOrderBy);
        for (int i = 0; i < numOrderBy; i++) {
            orderBy.add(Symbol.fromStream(in));
        }

        int numReverseFlags = in.readVInt();
        reverseFlags = new boolean[numReverseFlags];
        for (int i = 0; i < numReverseFlags; i++) {
            reverseFlags[i] = in.readBoolean();
        }

        int numNullsFirst = in.readVInt();
        nullsFirst = new Boolean[numNullsFirst];
        for (int i = 0; i < numNullsFirst; i++) {
            nullsFirst[i] = in.readOptionalBoolean();
        }

        limit = in.readVInt();
        offset = in.readVInt();

        whereClause = new WhereClause(in);

        int numPartitionBy = in.readVInt();
        partitionBy = new ArrayList<>(numPartitionBy);
        for (int i = 0; i < numPartitionBy; i++) {
            ReferenceInfo referenceInfo = new ReferenceInfo();
            referenceInfo.readFrom(in);
            partitionBy.add(referenceInfo);
        }
        if (in.readBoolean()) {
            scroll = Optional.of(Scroll.readScroll(in));
        } else {
            scroll = Optional.absent();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);

        out.writeVInt(outputs.size());
        for (Symbol output : outputs) {
            Symbol.toStream(output, out);
        }

        out.writeVInt(orderBy.size());
        for (Symbol symbol : orderBy) {
            Symbol.toStream(symbol, out);
        }

        out.writeVInt(reverseFlags.length);
        for (boolean reverseFlag : reverseFlags) {
            out.writeBoolean(reverseFlag);
        }

        out.writeVInt(nullsFirst.length);
        for (Boolean nullFirst : nullsFirst) {
            out.writeOptionalBoolean(nullFirst);
        }

        out.writeVInt(limit);
        out.writeVInt(offset);

        whereClause.writeTo(out);

        out.writeVInt(partitionBy.size());
        for (ReferenceInfo referenceInfo : partitionBy) {
            referenceInfo.writeTo(out);
        }

        out.writeBoolean(scroll.isPresent());
        if (scroll.isPresent()) {
            scroll.get().writeTo(out);
        }
    }

    public String index() {
        return index;
    }

    public int shardId() {
        return shard;
    }

    public List<? extends Symbol> outputs() {
        return outputs;
    }

    public List<Symbol> orderBy() {
        return orderBy;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public List<ReferenceInfo> partitionBy() {
        return partitionBy;
    }

    public Optional<Scroll> scroll() {
        return scroll;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryShardRequest)) return false;

        QueryShardRequest request = (QueryShardRequest) o;

        if (limit != request.limit) return false;
        if (offset != request.offset) return false;
        if (!index.equals(request.index)) return false;
        if (!Arrays.equals(nullsFirst, request.nullsFirst)) return false;
        if (!orderBy.equals(request.orderBy)) return false;
        if (!outputs.equals(request.outputs)) return false;
        if (!partitionBy.equals(request.partitionBy)) return false;
        if (!Arrays.equals(reverseFlags, request.reverseFlags)) return false;
        if (!shard.equals(request.shard)) return false;
        if (!whereClause.equals(request.whereClause)) return false;

        if (scroll.isPresent() && request.scroll.isPresent()) {
            if (!scroll.get().keepAlive().equals(request.scroll.get().keepAlive())) {
                return false;
            }
        } else if (scroll.isPresent() || request.scroll.isPresent()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shard.hashCode();
        result = 31 * result + outputs.hashCode();
        result = 31 * result + orderBy.hashCode();
        result = 31 * result + Arrays.hashCode(reverseFlags);
        result = 31 * result + Arrays.hashCode(nullsFirst);
        result = 31 * result + limit;
        result = 31 * result + offset;
        result = 31 * result + whereClause.hashCode();
        result = 31 * result + partitionBy.hashCode();
        result = 31 * result + scroll.hashCode(); // delegated by Optional
        return result;
    }
}
