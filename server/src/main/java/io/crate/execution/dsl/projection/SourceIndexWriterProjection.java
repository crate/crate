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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

/**
 * IndexWriterProjector that gets its values from a source input
 */
public class SourceIndexWriterProjection extends AbstractIndexWriterProjection {

    private static final String OVERWRITE_DUPLICATES = "overwrite_duplicates";
    private static final boolean OVERWRITE_DUPLICATES_DEFAULT = false;
    private static final String FAIL_FAST = "fail_fast";

    private final boolean failFast;
    private final Boolean overwriteDuplicates;
    private final Reference rawSourceReference;
    private final InputColumn rawSourceSymbol;
    private final List<? extends Symbol> outputs;


    @Nullable
    private final String[] excludes;

    public SourceIndexWriterProjection(RelationName relationName,
                                       @Nullable String partitionIdent,
                                       Reference rawSourceReference,
                                       InputColumn rawSourcePtr,
                                       List<ColumnIdent> primaryKeys,
                                       List<Symbol> partitionedBySymbols,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       Settings settings,
                                       @Nullable String[] excludes,
                                       List<Symbol> idSymbols,
                                       @Nullable Symbol clusteredBySymbol,
                                       List<? extends Symbol> outputs,
                                       boolean autoCreateIndices) {
        super(relationName, partitionIdent, primaryKeys, clusteredByColumn, settings, idSymbols, autoCreateIndices);
        this.rawSourceReference = rawSourceReference;
        this.excludes = excludes;
        this.partitionedBySymbols = partitionedBySymbols;
        this.clusteredBySymbol = clusteredBySymbol;
        this.rawSourceSymbol = rawSourcePtr;
        this.outputs = outputs;
        overwriteDuplicates = settings.getAsBoolean(OVERWRITE_DUPLICATES, OVERWRITE_DUPLICATES_DEFAULT);
        this.failFast = settings.getAsBoolean(FAIL_FAST, false);
    }

    SourceIndexWriterProjection(StreamInput in) throws IOException {
        super(in);
        Version version = in.getVersion();
        if (version.onOrAfter(Version.V_4_7_0)) {
            failFast = in.readBoolean();
        } else {
            failFast = false;
        }
        overwriteDuplicates = in.readBoolean();
        rawSourceReference = Reference.fromStream(in);
        rawSourceSymbol = (InputColumn) Symbols.fromStream(in);

        if (version.before(Version.V_5_3_0)) {
            if (in.readBoolean()) {
                // includes
                int length = in.readVInt();
                for (int i = 0; i < length; i++) {
                    in.readString();
                }
            }
        }
        if (in.readBoolean()) {
            int length = in.readVInt();
            excludes = new String[length];
            for (int i = 0; i < length; i++) {
                excludes[i] = in.readString();
            }
        } else {
            excludes = null;
        }
        outputs = Symbols.listFromStream(in);
        if (version.onOrAfter(Version.V_4_8_0) && version.before(Version.V_5_5_0)) {
            in.readBoolean(); // validation
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_4_7_0)) {
            out.writeBoolean(failFast);
        }
        out.writeBoolean(overwriteDuplicates);
        Reference.toStream(out, rawSourceReference);
        Symbols.toStream(rawSourceSymbol, out);

        if (version.before(Version.V_5_3_0)) {
            // no includes
            out.writeBoolean(false);
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
        Symbols.toStream(outputs, out);
        if (version.onOrAfter(Version.V_4_8_0) && version.before(Version.V_5_5_0)) {
            out.writeBoolean(true);
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
    public String[] excludes() {
        return excludes;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.INDEX_WRITER;
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SourceIndexWriterProjection that = (SourceIndexWriterProjection) o;
        return Objects.equals(overwriteDuplicates, that.overwriteDuplicates) &&
               Objects.equals(rawSourceReference, that.rawSourceReference) &&
               Objects.equals(rawSourceSymbol, that.rawSourceSymbol) &&
               Arrays.equals(excludes, that.excludes) &&
               failFast == that.failFast;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(),
                                  overwriteDuplicates,
                                  rawSourceReference,
                                  rawSourceSymbol,
                                  failFast);
        result = 31 * result + Arrays.hashCode(excludes);
        return result;
    }

    public boolean overwriteDuplicates() {
        return overwriteDuplicates;
    }

    public boolean failFast() {
        return failFast;
    }
}
