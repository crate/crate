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

import io.crate.analyze.CopyFromParserProperties;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class FileIndexWriterProjection extends Projection {

    private final int bulkActions;
    private final RelationName relationName;
    private final String partitionIdent;
    private final List<Reference> targetColumns;
    private final List<String> userTargets;

    @Nullable
    private final Reference clusteredByColumn;
    private final List<Reference> primaryKeyColumns;
    private final List<Reference> partitionedByColumns;

    private final boolean overwriteDuplicates;
    private final boolean autoCreateIndices;
    private final List<? extends Symbol> outputs;

    private final FileUriCollectPhase.InputFormat inputFormat;
    private final CopyFromParserProperties copyFromParserProperties;

    public FileIndexWriterProjection(RelationName relationName,
                                     @Nullable String partitionIdent,
                                     FileUriCollectPhase.InputFormat inputFormat,
                                     @NotNull List<Reference> targetColumns,
                                     List<String> userTargets,
                                     @NotNull List<Reference> partitionedByColumns,
                                     List<Reference> primaryKeyColumns,
                                     @Nullable Reference clusteredByColumn,
                                     Settings settings,
                                     List<? extends Symbol> outputs,
                                     boolean autoCreateIndices) {
        this.relationName = relationName;
        this.partitionIdent = partitionIdent;
        this.inputFormat = inputFormat;
        this.copyFromParserProperties = CopyFromParserProperties.of(settings);
        this.targetColumns = targetColumns;
        this.userTargets = userTargets;
        this.partitionedByColumns = partitionedByColumns;
        this.primaryKeyColumns = primaryKeyColumns;
        this.clusteredByColumn = clusteredByColumn;


        overwriteDuplicates = settings.getAsBoolean("overwrite_duplicates", false);
        this.autoCreateIndices = autoCreateIndices;
        bulkActions = settings.getAsInt("bulk_size", 10000);
        if (bulkActions <= 0) {
            throw new IllegalArgumentException("\"bulk_size\" must be greater than 0.");
        }
        this.outputs = outputs;
    }

    FileIndexWriterProjection(StreamInput in) throws IOException {
        relationName = new RelationName(in);
        partitionIdent = in.readOptionalString();
        inputFormat = in.readEnum(FileUriCollectPhase.InputFormat.class);
        copyFromParserProperties = new CopyFromParserProperties(in);
        targetColumns = in.readList(Reference::fromStream);
        userTargets = in.readStringList();

        partitionedByColumns = in.readList(Reference::fromStream);
        primaryKeyColumns = in.readList(Reference::fromStream);

        if (in.readBoolean()) {
            clusteredByColumn = Reference.fromStream(in);
        } else {
            clusteredByColumn = null;
        }

        bulkActions = in.readVInt();
        autoCreateIndices = in.readBoolean();

        overwriteDuplicates = in.readBoolean();
        outputs = Symbols.listFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        relationName.writeTo(out);
        out.writeOptionalString(partitionIdent);
        out.writeEnum(inputFormat);
        copyFromParserProperties.writeTo(out);
        out.writeCollection(targetColumns, Reference::toStream);
        out.writeStringCollection(userTargets);

        out.writeCollection(partitionedByColumns, Reference::toStream);
        out.writeCollection(primaryKeyColumns, Reference::toStream);

        if (clusteredByColumn == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Reference.toStream(out, clusteredByColumn);
        }

        out.writeVInt(bulkActions);
        out.writeBoolean(autoCreateIndices);

        out.writeBoolean(overwriteDuplicates);
        Symbols.toStream(outputs, out);
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFileIndexWriterProjection(this, context);
    }

    public RelationName tableIdent() {
        return relationName;
    }

    @Nullable
    public String partitionIdent() {
        return partitionIdent;
    }

    public List<Reference> targetColumns() {
        return targetColumns;
    }

    public List<String> userTargets() {
        return userTargets;
    }

    public List<Reference> partitionedByColumns() {
        return partitionedByColumns;
    }

    public List<Reference> primaryKeyColumns() {
        return primaryKeyColumns;
    }

    @Nullable
    public Reference clusteredByColumn() {
        return clusteredByColumn;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FILE_INDEX_WRITER;
    }

    public boolean overwriteDuplicates() {
        return overwriteDuplicates;
    }

    public int bulkActions() {
        return bulkActions;
    }

    public FileUriCollectPhase.InputFormat inputFormat() {
        return inputFormat;
    }

    public CopyFromParserProperties copyFromParserProperties() {
        return copyFromParserProperties;
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    public boolean autoCreateIndices() {
        return autoCreateIndices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileIndexWriterProjection that = (FileIndexWriterProjection) o;
        return Objects.equals(overwriteDuplicates, that.overwriteDuplicates) &&
            Objects.equals(targetColumns, that.targetColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
            overwriteDuplicates,
            targetColumns);
    }


}
