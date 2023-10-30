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

package io.crate.analyze;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.RelationsUnknown;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DropViewAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testDropViewContainsIfExistsAndEmptyViews() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();

        AnalyzedDropView dropView = e.analyze("drop view if exists v1, v2, x.v3");

        assertThat(dropView.ifExists()).isTrue();
        assertThat(dropView.views()).isEmpty();
    }

    @Test
    public void testDropViewRaisesRelationsUnkownForMissingView() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();

        assertThatThrownBy(() -> e.analyze("drop view v1, v2"))
            .isExactlyInstanceOf(RelationsUnknown.class)
            .hasMessage("Relations not found: doc.v1, doc.v2");
    }
}
