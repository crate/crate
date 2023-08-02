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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;

public class FileParsingProjection extends Projection {

    private final List<Reference> allTargetColumns;
    private final List<? extends Symbol> outputs;
    private final FileUriCollectPhase.InputFormat inputFormat;
    private final CopyFromParserProperties copyFromParserProperties;


    public FileParsingProjection(List<Reference> allTargetColumns,
                                 FileUriCollectPhase.InputFormat inputFormat,
                                 CopyFromParserProperties copyFromParserProperties,
                                 List<? extends Symbol> outputs) {
        this.allTargetColumns = allTargetColumns;
        this.inputFormat = inputFormat;
        this.copyFromParserProperties = copyFromParserProperties;
        this.outputs = outputs;
    }

    FileParsingProjection(StreamInput in) throws IOException {
        allTargetColumns = in.readList(Reference::fromStream);
        inputFormat = in.readEnum(FileUriCollectPhase.InputFormat.class);
        copyFromParserProperties = new CopyFromParserProperties(in);
        outputs = Symbols.listFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(allTargetColumns, Reference::toStream);
        out.writeEnum(inputFormat);
        copyFromParserProperties.writeTo(out);
        Symbols.toStream(outputs, out);
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFileParsingProjection(this, context);
    }

    public List<Reference> allTargetColumns() {
        return allTargetColumns;
    }

    public FileUriCollectPhase.InputFormat inputFormat() {
        return inputFormat;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FILE_PARSING_PROJECTION;
    }

    public CopyFromParserProperties copyFromParserProperties() {
        return copyFromParserProperties;
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileParsingProjection that = (FileParsingProjection) o;
        return allTargetColumns.equals(that.allTargetColumns) &&
            inputFormat.equals(that.inputFormat) &&
            copyFromParserProperties.equals(that.copyFromParserProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
            allTargetColumns,
            inputFormat,
            copyFromParserProperties
        );
    }

}
