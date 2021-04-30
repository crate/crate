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
import java.util.List;
import java.util.Objects;

public class SourceIndexWriterReturnSummaryProjection extends SourceIndexWriterProjection {

    private final InputColumn sourceUri;
    private final InputColumn sourceUriFailure;
    private final InputColumn lineNumber;

    public SourceIndexWriterReturnSummaryProjection(RelationName relationName,
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
                                                    List<? extends Symbol> outputs,
                                                    boolean autoCreateIndices,
                                                    InputColumn sourceUri,
                                                    InputColumn sourceUriFailure,
                                                    InputColumn lineNumber) {
        super(relationName,partitionIdent, rawSourceReference, rawSourcePtr, primaryKeys, partitionedBySymbols,
            clusteredByColumn, settings, includes, excludes, idSymbols, clusteredBySymbol, outputs, autoCreateIndices);
        this.sourceUri = sourceUri;
        this.sourceUriFailure = sourceUriFailure;
        this.lineNumber = lineNumber;
    }

    SourceIndexWriterReturnSummaryProjection(StreamInput in) throws IOException {
        super(in);
        sourceUri = (InputColumn) Symbols.fromStream(in);
        sourceUriFailure = (InputColumn) Symbols.fromStream(in);
        lineNumber = (InputColumn) Symbols.fromStream(in);
    }

    public InputColumn sourceUri() {
        return sourceUri;
    }

    public InputColumn sourceUriFailure() {
        return sourceUriFailure;
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
        Symbols.toStream(sourceUri, out);
        Symbols.toStream(sourceUriFailure, out);
        Symbols.toStream(lineNumber, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SourceIndexWriterReturnSummaryProjection that = (SourceIndexWriterReturnSummaryProjection) o;
        return Objects.equals(sourceUri, that.sourceUri) &&
               Objects.equals(sourceUriFailure, that.sourceUriFailure) &&
               Objects.equals(lineNumber, that.lineNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sourceUri, sourceUriFailure, lineNumber);
    }
}
