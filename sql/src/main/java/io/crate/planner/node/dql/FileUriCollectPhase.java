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

package io.crate.planner.node.dql;

import com.google.common.base.MoreObjects;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.TransactionContext;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class FileUriCollectPhase extends AbstractProjectionsPhase implements CollectPhase {

    public static final ExecutionPhaseFactory<FileUriCollectPhase> FACTORY = new ExecutionPhaseFactory<FileUriCollectPhase>() {
        @Override
        public FileUriCollectPhase create() {
            return new FileUriCollectPhase();
        }
    };

    private Collection<String> executionNodes;
    private Symbol targetUri;
    private List<Symbol> toCollect;
    private String compression;
    private Boolean sharedStorage;
    private DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

    private FileUriCollectPhase() {
        super();
    }

    public FileUriCollectPhase(UUID jobId,
                               int phaseId,
                               String name,
                               Collection<String> executionNodes,
                               Symbol targetUri,
                               List<Symbol> toCollect,
                               List<Projection> projections,
                               String compression,
                               Boolean sharedStorage) {
        super(jobId, phaseId, name, projections);
        this.executionNodes = executionNodes;
        this.targetUri = targetUri;
        this.toCollect = toCollect;
        this.compression = compression;
        this.sharedStorage = sharedStorage;
        outputTypes = extractOutputTypes(toCollect, projections);
    }

    public Symbol targetUri() {
        return targetUri;
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

    public FileUriCollectPhase normalize(EvaluatingNormalizer normalizer, TransactionContext transactionContext) {
        List<Symbol> normalizedToCollect = normalizer.normalize(toCollect(), transactionContext);
        Symbol normalizedTargetUri = normalizer.normalize(targetUri, transactionContext);
        boolean changed = normalizedToCollect != toCollect() || (normalizedTargetUri != targetUri);
        if (!changed) {
            return this;
        }
        return new FileUriCollectPhase(
            jobId(),
            phaseId(),
            name(),
            executionNodes,
            normalizedTargetUri,
            normalizedToCollect,
            projections(),
            compression(),
            sharedStorage());
    }

    @Nullable
    public String compression() {
        return compression;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        compression = in.readOptionalString();
        sharedStorage = in.readOptionalBoolean();
        targetUri = Symbols.fromStream(in);

        int numNodes = in.readVInt();
        List<String> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            nodes.add(in.readString());
        }
        this.executionNodes = nodes;
        toCollect = Symbols.listFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(compression);
        out.writeOptionalBoolean(sharedStorage);
        Symbols.toStream(targetUri, out);
        out.writeVInt(executionNodes.size());
        for (String node : executionNodes) {
            out.writeString(node);
        }
        Symbols.toStream(toCollect, out);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name())
            .add("targetUri", targetUri)
            .add("projections", projections)
            .add("outputTypes", outputTypes)
            .add("compression", compression)
            .add("sharedStorageDefault", sharedStorage)
            .toString();
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
}

