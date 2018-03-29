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

package io.crate.execution.dsl.projection;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * IndexWriterProjector that gets its values from a source input
 */
public class SourceIndexWriterProjection extends AbstractIndexWriterProjection {

    private final Boolean overwriteDuplicates;

    private final Reference rawSourceReference;
    private final InputColumn rawSourceSymbol;

    private static final String OVERWRITE_DUPLICATES = "overwrite_duplicates";
    private static final boolean OVERWRITE_DUPLICATES_DEFAULT = false;

    @Nullable
    private String[] includes;

    @Nullable
    private String[] excludes;


    public SourceIndexWriterProjection(RelationName relationName,
                                       @Nullable String partitionIdent,
                                       Reference rawSourceReference,
                                       InputColumn rawSourcePtr,
                                       List<ColumnIdent> primaryKeys,
                                       List<Symbol> partitionedBySymbols,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       Settings settings,
                                       @Nullable String[] includes,
                                       @Nullable String[] excludes,
                                       List<Symbol> idSymbols,
                                       @Nullable Symbol clusteredBySymbol,
                                       boolean autoCreateIndices) {
        super(relationName, partitionIdent, primaryKeys, clusteredByColumn, settings, idSymbols, autoCreateIndices);
        this.rawSourceReference = rawSourceReference;
        this.includes = includes;
        this.excludes = excludes;
        this.partitionedBySymbols = partitionedBySymbols;
        this.clusteredBySymbol = clusteredBySymbol;
        this.rawSourceSymbol = rawSourcePtr;
        overwriteDuplicates = settings.getAsBoolean(OVERWRITE_DUPLICATES, OVERWRITE_DUPLICATES_DEFAULT);
    }

    public SourceIndexWriterProjection(StreamInput in) throws IOException {
        super(in);
        overwriteDuplicates = in.readBoolean();
        rawSourceReference = Reference.fromStream(in);
        rawSourceSymbol = (InputColumn) Symbols.fromStream(in);


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
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitSourceIndexWriterProjection(this, context);
    }

    public InputColumn rawSource() {
        return rawSourceSymbol;
    }

    public Reference rawSourceReference() {
        return rawSourceReference;
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
    public void replaceSymbols(Function<? super Symbol, ? extends Symbol> replaceFunction) {
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.INDEX_WRITER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SourceIndexWriterProjection that = (SourceIndexWriterProjection) o;

        if (!Arrays.equals(excludes, that.excludes)) return false;
        if (!Arrays.equals(includes, that.includes)) return false;
        return rawSourceSymbol.equals(that.rawSourceSymbol);
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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeBoolean(overwriteDuplicates);
        Reference.toStream(rawSourceReference, out);
        Symbols.toStream(rawSourceSymbol, out);

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

    public boolean overwriteDuplicates() {
        return overwriteDuplicates;
    }
}
