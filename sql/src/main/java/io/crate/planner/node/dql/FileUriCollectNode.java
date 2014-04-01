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

import com.google.common.base.Objects;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class FileUriCollectNode extends CollectNode {

    private Symbol targetUri;
    private boolean compressed;

    public FileUriCollectNode() {

    }

    public FileUriCollectNode(String id,
                              Routing routing,
                              Symbol targetUri,
                              List<Symbol> toCollect,
                              List<Projection> projections) {
        super(id, routing, toCollect, projections);
        this.targetUri = targetUri;
        this.compressed = false;
    }

    public Symbol targetUri() {
        return targetUri;
    }

    public FileReadingCollector.FileFormat fileFormat() {
        return FileReadingCollector.FileFormat.JSON;
    }

    public void compressed(boolean compressed) {
        this.compressed = compressed;
    }

    @Override
    public FileUriCollectNode normalize(EvaluatingNormalizer normalizer) {
        List<Symbol> normalizedToCollect = normalizer.normalize(toCollect());
        Symbol normalizedTargetUri = normalizer.normalize(targetUri);
        WhereClause normalizedWhereClause = whereClause().normalize(normalizer);
        boolean changed =
                (normalizedToCollect != toCollect() )
                        || (normalizedTargetUri != targetUri)
                        || (normalizedWhereClause != whereClause());
        if (!changed) {
            return this;
        }
        FileUriCollectNode result = new FileUriCollectNode(
                id(), routing(), normalizedTargetUri, normalizedToCollect, projections());
        result.downStreamNodes(downStreamNodes());
        result.maxRowGranularity(maxRowGranularity());
        result.whereClause(normalizedWhereClause);
        if (jobId().isPresent()) {
            result.jobId(jobId().get());
        }
        return result;
    }

    public boolean compressed() {
        return compressed;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        compressed = in.readBoolean();
        targetUri = Symbol.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(compressed);
        Symbol.toStream(targetUri, out);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id())
                .add("targetUri", targetUri)
                .add("projections", projections)
                .add("outputTypes", outputTypes)
                .add("compressed", compressed)
                .toString();
    }
}

