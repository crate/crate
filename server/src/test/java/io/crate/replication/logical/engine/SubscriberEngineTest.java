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

import static org.elasticsearch.index.engine.EngineTestCase.newUid;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import org.assertj.core.api.Assertions;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SubscriberEngineTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_operation_validation_with_unassigned_seqno_raises_exception() {
        var op = new Engine.Index(newUid("0"), 0, InternalEngineTests.createParsedDoc("0"));
        Assertions.assertThatThrownBy(() -> SubscriberEngine.validate(op))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessageContaining("A subscriber engine does not accept operations without an assigned sequence number");
    }

    @Test
    public void test_operation_validation_with_invalid_version_type_raises_exception() {
        var op = new Engine.Index(newUid("0"),
                                  InternalEngineTests.createParsedDoc("0"),
                                  0,
                                  0,
                                  Versions.MATCH_ANY,
                                  VersionType.INTERNAL,
                                  Engine.Operation.Origin.PRIMARY,
                                  System.nanoTime(),
                                  -1,
                                  false,
                                  UNASSIGNED_SEQ_NO,
                                  0);
        Assertions.assertThatThrownBy(() -> SubscriberEngine.validate(op))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Invalid version_type in a subscriber engine; version_type=INTERNAL origin=PRIMARY");
    }
}
