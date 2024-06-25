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
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class SourceIndexWriterReturnSummaryProjection extends SourceIndexWriterProjection {

    private final InputColumn sourceUri;
    private final InputColumn sourceUriFailure;
    private final InputColumn sourceParsingFailure;
    private final InputColumn lineNumber;

    public SourceIndexWriterReturnSummaryProjection(RelationName relationName,
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
                                                    boolean autoCreateIndices,
                                                    InputColumn sourceUri,
                                                    InputColumn sourceUriFailure,
                                                    InputColumn sourceParsingFailure,
                                                    InputColumn lineNumber) {
        super(relationName,partitionIdent, rawSourceReference, rawSourcePtr, primaryKeys, partitionedBySymbols,
            clusteredByColumn, settings, excludes, idSymbols, clusteredBySymbol, outputs, autoCreateIndices);
        this.sourceUri = sourceUri;
        this.sourceUriFailure = sourceUriFailure;
        this.sourceParsingFailure = sourceParsingFailure;
        this.lineNumber = lineNumber;
    }

    SourceIndexWriterReturnSummaryProjection(StreamInput in) throws IOException {
        super(in);
        sourceUri = (InputColumn) Symbol.fromStream(in);
        sourceUriFailure = (InputColumn) Symbol.fromStream(in);
        lineNumber = (InputColumn) Symbol.fromStream(in);
        // From 4.7.2 we differentiate IO and non-io failure
        // as the former is supposed to happen only once and the latter can happen multiple times per URI.
        sourceParsingFailure = in.getVersion().before(Version.V_4_7_1) ? null : (InputColumn) Symbol.fromStream(in);
    }

    public InputColumn sourceUri() {
        return sourceUri;
    }

    public InputColumn sourceUriFailure() {
        return sourceUriFailure;
    }

    @Nullable
    public InputColumn sourceParsingFailure() {
        return sourceParsingFailure;
    }

    public InputColumn lineNumber() {
        return lineNumber;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.INDEX_WRITER_RETURN_SUMMARY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Symbol.toStream(sourceUri, out);
        Symbol.toStream(sourceUriFailure, out);
        Symbol.toStream(lineNumber, out);
        if (out.getVersion().after(Version.V_4_7_1)) {
            Symbol.toStream(sourceParsingFailure, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SourceIndexWriterReturnSummaryProjection that = (SourceIndexWriterReturnSummaryProjection) o;
        return Objects.equals(sourceUri, that.sourceUri) &&
               Objects.equals(sourceUriFailure, that.sourceUriFailure) &&
               Objects.equals(sourceParsingFailure, that.sourceParsingFailure) &&
               Objects.equals(lineNumber, that.lineNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sourceUri, sourceUriFailure, sourceParsingFailure, lineNumber);
    }
}
