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
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RelationName;
import io.crate.planner.distribution.DistributionInfo;

public class ForeignCollectPhase extends AbstractProjectionsPhase implements CollectPhase {

    private final String handlerNode;
    private final RelationName relationName;
    private final List<Symbol> toCollect;

    public ForeignCollectPhase(UUID jobId,
                               int phaseId,
                               String handlerNode,
                               RelationName relationName,
                               List<Symbol> toCollect) {
        super(jobId, phaseId, relationName.fqn(), null);
        this.handlerNode = handlerNode;
        this.relationName = relationName;
        this.toCollect = toCollect;
        this.outputTypes = Symbols.typeView(toCollect);
    }

    public ForeignCollectPhase(StreamInput in) throws IOException {
        super(in);
        this.handlerNode = in.readString();
        this.relationName = new RelationName(in);
        this.toCollect = Symbols.listFromStream(in);
        this.outputTypes = extractOutputTypes(toCollect, projections);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(handlerNode);
        relationName.writeTo(out);
        Symbols.toStream(toCollect, out);
    }

    @Override
    public DistributionInfo distributionInfo() {
        return DistributionInfo.DEFAULT_BROADCAST;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        throw new UnsupportedOperationException("Cannot set distributionInfo on ForeignCollectPhase");
    }

    @Override
    public Type type() {
        return Type.FOREIGN_COLLECT;
    }

    @Override
    public Collection<String> nodeIds() {
        return List.of(handlerNode);
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitForeignCollect(this, context);
    }

    @Override
    public List<Symbol> toCollect() {
        return toCollect;
    }

    public RelationName relationName() {
        return relationName;
    }
}
