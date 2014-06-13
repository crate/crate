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

package io.crate.planner.projection;

import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * IndexWriterProjector that gets its values from a source input
 */
public class SourceIndexWriterProjection extends AbstractIndexWriterProjection {

    private @Nullable String[] includes;
    private @Nullable String[] excludes;

    protected Symbol rawSourceSymbol;

    public static final ProjectionFactory<SourceIndexWriterProjection> FACTORY =
            new ProjectionFactory<SourceIndexWriterProjection>() {
                @Override
                public SourceIndexWriterProjection newInstance() {
                    return new SourceIndexWriterProjection();
                }
            };

    protected SourceIndexWriterProjection() {}

    public SourceIndexWriterProjection(String tableName,
                                       List<ColumnIdent> primaryKeys,
                                       List<ColumnIdent> partitionedBy,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       int clusteredByIdx,
                                       Settings settings,
                                       @Nullable String[] includes,
                                       @Nullable String[] excludes) {
        super(tableName, primaryKeys, clusteredByColumn, settings);

        this.includes = includes;
        this.excludes = excludes;
        int[] pkIndices = new int[primaryKeys.size()];
        for (int i = 0; i<pkIndices.length;i++) {
            pkIndices[i]=i;
        }
        int currentInputIndex = primaryKeys.size();
        int[] partitionedByIndices = new int[partitionedBy.size()];

        for (int i = 0, length = partitionedBy.size(); i < length; i++) {
            int idx = primaryKeys.indexOf(partitionedBy.get(i));
            if (idx == -1) {
                idx = currentInputIndex;
                currentInputIndex++;
            }
            partitionedByIndices[i] = idx;
        }

        if (clusteredByIdx == -1) {
            clusteredByIdx = currentInputIndex;
            currentInputIndex++;
        }

        generateSymbols(pkIndices, partitionedByIndices, clusteredByIdx);
        rawSourceSymbol = new InputColumn(currentInputIndex);
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitSourceIndexWriterProjection(this, context);
    }
    public Symbol rawSource() {
        return rawSourceSymbol;
    }

    @Nullable
    public String[] includes() {
        return includes;
    }

    @Nullable
    public String[] excludes() {
        return excludes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SourceIndexWriterProjection that = (SourceIndexWriterProjection) o;

        if (!Arrays.equals(excludes, that.excludes)) return false;
        if (!Arrays.equals(includes, that.includes)) return false;
        if (!rawSourceSymbol.equals(that.rawSourceSymbol)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (includes != null ? Arrays.hashCode(includes) : 0);
        result = 31 * result + (excludes != null ? Arrays.hashCode(excludes) : 0);
        result = 31 * result + rawSourceSymbol.hashCode();
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        rawSourceSymbol = Symbol.fromStream(in);


        if (in.readBoolean()) {
            int length = in.readVInt();
            includes = new String[length];
            for (int i = 0; i < length; i++) {
                includes[i] = in.readString();
            }
        }
        if (in.readBoolean()) {
            int length = in.readVInt();
            excludes = new String[length];
            for (int i = 0; i < length; i++) {
                excludes[i] = in.readString();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        Symbol.toStream(rawSourceSymbol, out);

        if (includes == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(includes.length);
            for (String include : includes) {
                out.writeString(include);
            }
        }
        if (excludes == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(excludes.length);
            for (String exclude : excludes) {
                out.writeString(exclude);
            }
        }
    }
}
