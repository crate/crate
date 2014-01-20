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

package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class ReferenceInfo implements Comparable<ReferenceInfo>, Streamable {

    private ReferenceIdent ident;
    private DataType type;
    private RowGranularity granularity;

    public ReferenceInfo() {

    }

    public ReferenceInfo(ReferenceIdent ident, RowGranularity granularity, DataType type) {
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
    }

    public ReferenceIdent ident() {
        return ident;
    }

    public DataType type() {
        return type;
    }

    public RowGranularity granularity() {
        return granularity;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ReferenceInfo o = (ReferenceInfo) obj;
        return Objects.equal(granularity, o.granularity) &&
                Objects.equal(ident, o.ident) &&
                Objects.equal(type, o.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(granularity, ident, type);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("granularity", type)
                .add("ident", ident)
                .add("type", type)
                .toString();
    }

    @Override
    public int compareTo(ReferenceInfo o) {
        return ComparisonChain.start()
                .compare(granularity, o.granularity)
                .compare(ident, o.ident)
                .compare(type, o.type)
                .result();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ident = new ReferenceIdent();
        ident.readFrom(in);
        type = DataType.fromStream(in);
        granularity = RowGranularity.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataType.toStream(type, out);
        RowGranularity.toStream(granularity, out);
    }
}
