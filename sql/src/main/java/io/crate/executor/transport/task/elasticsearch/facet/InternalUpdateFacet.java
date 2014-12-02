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

package io.crate.executor.transport.task.elasticsearch.facet;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.Locale;

public class InternalUpdateFacet extends InternalFacet implements UpdateFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes(TYPE));

    private ReduceContext reduceContext;
    private Object[][] rows;
    private long rowCount;

    public InternalUpdateFacet(String facetName) {
        super(facetName);
    }

    public InternalUpdateFacet() {
        super();
    }


    public static void registerStreams() {
        Streams.registerStream(new SQLFacetStream(), STREAM_TYPE);
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    /**
     * This method is called on the collecting node. In this implementation no reduction is done
     * here, since the statement is not known
     * the real reduce happens in {@link UpdateFacet#reduce()}
     */
    @Override
    public Facet reduce(ReduceContext reduceContext) {
        this.reduceContext = reduceContext;
        return this;
    }

    @Override
    public String getType() {
        return UpdateFacet.TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        rowCount = in.readVLong();
        if (rowCount == 0) return;
        int numRows = in.readInt();
        if (numRows == 0) return;
        int numCols = in.readInt();
        rows = new Object[numRows][numCols];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                rows[i][j] = in.readGenericValue();
            }
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(rowCount);
        if (rowCount == 0) return;
        int numCols = 0;
        if (rows == null || rows.length == 0) {
            out.writeInt(0);
            return;
        } else {
            out.writeInt(rows.length);
            numCols = rows[0].length;
        }
        for (int i = 0; i < rows.length; i++) {
            for (int j = 0; j < numCols; j++) {
                out.writeGenericValue(rows[i][j]);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // at this point we do not support xcontent output
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "toXContent not supported on %s", getClass().getSimpleName()));
    }

    public static InternalUpdateFacet readMapReduceFacet(StreamInput in) throws IOException {
        InternalUpdateFacet facet = new InternalUpdateFacet();
        facet.readFrom(in);
        return facet;
    }

    public void reduce() {
        // Currently only the rowcount gets accumulated
        for (Facet facet : reduceContext.facets()) {
            if (facet != this) {
                rowCount += ((InternalUpdateFacet) facet).rowCount();
            }
        }
    }

    public long rowCount() {
        return rowCount;
    }

    public void rowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    private static class SQLFacetStream implements InternalFacet.Stream {

        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            // this gets executed on the HandlerNode, where the statement should have already
            // been parsed.
            return InternalUpdateFacet.readMapReduceFacet(in);
        }
    }
}
