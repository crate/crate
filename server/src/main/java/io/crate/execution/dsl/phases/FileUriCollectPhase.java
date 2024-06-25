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

package io.crate.execution.dsl.phases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.distribution.DistributionInfo;

public class FileUriCollectPhase extends AbstractProjectionsPhase implements CollectPhase {

    private final Collection<String> executionNodes;
    private final Symbol targetUri;
    private final List<String> targetColumns;
    private final List<Symbol> toCollect;
    private final CopyFromParserProperties parserProperties;
    private final String compression;
    private final Boolean sharedStorage;
    private DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;
    private final InputFormat inputFormat;
    private final Settings withClauseOptions;

    public FileUriCollectPhase(UUID jobId,
                               int phaseId,
                               String name,
                               Collection<String> executionNodes,
                               Symbol targetUri,
                               List<String> targetColumns,
                               List<Symbol> toCollect,
                               List<Projection> projections,
                               String compression,
                               Boolean sharedStorage,
                               CopyFromParserProperties parserProperties,
                               InputFormat inputFormat,
                               Settings withClauseOptions) {
        super(jobId, phaseId, name, projections);
        this.executionNodes = executionNodes;
        this.targetUri = targetUri;
        this.targetColumns = targetColumns;
        this.toCollect = toCollect;
        this.compression = compression;
        this.sharedStorage = sharedStorage;
        this.parserProperties = parserProperties;
        this.inputFormat = inputFormat;
        outputTypes = extractOutputTypes(toCollect, projections);
        this.withClauseOptions = withClauseOptions;
    }

    public enum InputFormat {
        JSON,
        CSV
    }

    public Symbol targetUri() {
        return targetUri;
    }

    public List<String> targetColumns() {
        return targetColumns;
    }

    @Override
    public Collection<String> nodeIds() {
        return executionNodes;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitFileUriCollectPhase(this, context);
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    @Override
    public Type type() {
        return Type.FILE_URI_COLLECT;
    }

    @Nullable
    public String compression() {
        return compression;
    }

    public InputFormat inputFormat() {
        return inputFormat;
    }

    public CopyFromParserProperties parserProperties() {
        return parserProperties;
    }

    public FileUriCollectPhase(StreamInput in) throws IOException {
        super(in);
        compression = in.readOptionalString();
        sharedStorage = in.readOptionalBoolean();
        targetUri = Symbol.fromStream(in);
        int numNodes = in.readVInt();
        List<String> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            nodes.add(in.readString());
        }
        this.executionNodes = nodes;
        if (in.getVersion().onOrAfter(Version.V_4_8_0)) {
            targetColumns = in.readList(StreamInput::readString);
        } else {
            targetColumns = List.of();
        }
        toCollect = Symbols.fromStream(in);
        inputFormat = InputFormat.values()[in.readVInt()];
        if (in.getVersion().onOrAfter(Version.V_4_4_0)) {
            parserProperties = new CopyFromParserProperties(in);
        } else {
            parserProperties = CopyFromParserProperties.DEFAULT;
        }
        if (in.getVersion().onOrAfter(Version.V_4_8_0)) {
            withClauseOptions = Settings.readSettingsFromStream(in);
        } else {
            withClauseOptions = Settings.EMPTY;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(compression);
        out.writeOptionalBoolean(sharedStorage);
        Symbol.toStream(targetUri, out);
        out.writeVInt(executionNodes.size());
        for (String node : executionNodes) {
            out.writeString(node);
        }
        if (out.getVersion().onOrAfter(Version.V_4_8_0)) {
            out.writeStringCollection(targetColumns);
        }
        Symbols.toStream(toCollect, out);
        out.writeVInt(inputFormat.ordinal());
        if (out.getVersion().onOrAfter(Version.V_4_4_0)) {
            parserProperties.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_4_8_0)) {
            Settings.writeSettingsToStream(out, withClauseOptions);
        }
    }

    @Nullable
    public Boolean sharedStorage() {
        return sharedStorage;
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    public Settings withClauseOptions() {
        return withClauseOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FileUriCollectPhase that = (FileUriCollectPhase) o;
        return Objects.equals(executionNodes, that.executionNodes) &&
               Objects.equals(targetUri, that.targetUri) &&
               Objects.equals(targetColumns, that.targetColumns) &&
               Objects.equals(toCollect, that.toCollect) &&
               Objects.equals(parserProperties, that.parserProperties) &&
               Objects.equals(compression, that.compression) &&
               Objects.equals(sharedStorage, that.sharedStorage) &&
               Objects.equals(distributionInfo, that.distributionInfo) &&
               inputFormat == that.inputFormat &&
               Objects.equals(withClauseOptions, that.withClauseOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            executionNodes,
            targetUri,
            targetColumns,
            toCollect,
            parserProperties,
            compression,
            sharedStorage,
            distributionInfo,
            inputFormat,
            withClauseOptions);
    }

    @Override
    public String toString() {
        return "FileUriCollectPhase{" +
               "executionNodes=" + executionNodes +
               ", targetUri=" + targetUri +
               ", targetColumns=" + targetColumns +
               ", toCollect=" + toCollect +
               ", parserProperties=" + parserProperties +
               ", compression='" + compression + '\'' +
               ", sharedStorage=" + sharedStorage +
               ", distributionInfo=" + distributionInfo +
               ", inputFormat=" + inputFormat +
               ", withClauseOptions{" + withClauseOptions.toString() +
               '}';
    }
}
