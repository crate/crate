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

import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * IndexWriterProjector that gets its values from a source input
 */
public class SourceIndexWriterProjection extends AbstractIndexWriterProjection {

    private Boolean overwriteDuplicates;
    private
    @Nullable
    String[] includes;
    private
    @Nullable
    String[] excludes;

    protected Reference rawSourceReference;
    protected InputColumn rawSourceSymbol;

    private final static String OVERWRITE_DUPLICATES = "overwrite_duplicates";
    private final static boolean OVERWRITE_DUPLICATES_DEFAULT = false;

    public SourceIndexWriterProjection(TableIdent tableIdent,
                                       @Nullable String partitionIdent,
                                       Reference rawSourceReference,
                                       List<ColumnIdent> primaryKeys,
                                       List<ColumnIdent> partitionedBy,
                                       List<BytesRef> partitionValues,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       int clusteredByIdx,
                                       Settings settings,
                                       @Nullable String[] includes,
                                       @Nullable String[] excludes,
                                       boolean autoCreateIndices) {
        super(tableIdent, partitionIdent, primaryKeys, clusteredByColumn, settings, autoCreateIndices);

        this.rawSourceReference = rawSourceReference;
        this.includes = includes;
        this.excludes = excludes;

        int currentInputIndex = primaryKeys.size();

        // "_id" is always generated, no need to try to parse it from the source.
        if (primaryKeys.size() == 1 && primaryKeys.get(0).equals(DocSysColumns.ID)) {
            idSymbols = Collections.emptyList();
        } else {
            List<Symbol> idSymbols = new ArrayList<>(primaryKeys.size());
            for (int i = 0; i < primaryKeys.size(); i++) {
                InputColumn ic = new InputColumn(i, null);
                idSymbols.add(ic);
                if (i == clusteredByIdx) {
                    clusteredBySymbol = ic;
                }
            }
            this.idSymbols = idSymbols;
        }

        List<Symbol> partitionedBySymbols = new ArrayList<>(partitionedBy.size());
        for (int i = 0, length = partitionedBy.size(); i < length; i++) {
            int idx = primaryKeys.indexOf(partitionedBy.get(i));
            Symbol partitionSymbol;
            if (partitionValues.size() > i) {
                // copy from into partition, do NOT define partition symbols
                if (idx > -1) {
                    // copy from into partition where partitioned column is a primary key
                    // set partition value as primary key input
                    idSymbols.set(idx, Literal.of(partitionValues.get(i)));
                }
                continue;
            }
            if (idx == -1) {
                partitionSymbol = new InputColumn(currentInputIndex++, null);
            } else {
                // partition column is part of primary key, use primary key input
                partitionSymbol = idSymbols.get(idx);
            }
            partitionedBySymbols.add(partitionSymbol);
        }
        this.partitionedBySymbols = partitionedBySymbols;

        // if clusteredByColumn equals _id then the routing is implicit.
        if (clusteredByIdx == -1 && clusteredByColumn != null && !DocSysColumns.ID.equals(clusteredByColumn)) {
            clusteredBySymbol = new InputColumn(currentInputIndex++, null);
        }

        overwriteDuplicates = settings.getAsBoolean(OVERWRITE_DUPLICATES, OVERWRITE_DUPLICATES_DEFAULT);
        rawSourceSymbol = new InputColumn(currentInputIndex, DataTypes.STRING);
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
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
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
