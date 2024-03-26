/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.engine;

import java.io.IOException;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.exceptions.UnsupportedFeatureException;

public class SubscriberEngine extends InternalEngine {

    public SubscriberEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    @Override
    protected long generateSeqNoForOperationOnPrimary(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        assert operation.seqNo() >= 0 : "replicated operation should have an assigned seq no. but was: " + operation.seqNo();
        markSeqNoAsSeen(operation.seqNo());
        return operation.seqNo();
    }

    @Override
    protected boolean assertPrimaryIncomingSequenceNumber(Operation.Origin origin, long seqNo) {
        assert origin == Origin.PRIMARY : "Expected origin PRIMARY for replicated operations, but was: " + origin;
        assert seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO :
            "Expected valid sequence number for replicated op but was unassigned";
        return true;
    }

    @VisibleForTesting
    static void validate(final Operation operation) {
        if (operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new UnsupportedFeatureException("A subscriber engine does not accept operations without an " +
                                                  "assigned sequence number");
        }
        if ((operation.origin() == Engine.Operation.Origin.PRIMARY) != (operation.versionType() == VersionType.EXTERNAL)) {
            throw new IllegalStateException("Invalid version_type in a subscriber engine; version_type="
                                            + operation.versionType() + " origin=" + operation.origin());
        }
    }

    @Override
    protected IndexingStrategy indexingStrategyForOperation(Index index) throws IOException {
        validate(index);
        return planIndexingAsNonPrimary(index);
    }

    @Override
    protected DeletionStrategy deletionStrategyForOperation(Delete delete) throws IOException {
        validate(delete);
        return super.deletionStrategyForOperation(delete);
    }

    @Override
    protected boolean assertNonPrimaryOrigin(Operation operation) {
        return true;
    }
}
