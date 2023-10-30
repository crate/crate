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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableDropCheckConstraintAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (" +
                      "     id integer primary key," +
                      "     qty integer constraint check_qty_gt_zero check (qty > 0)," +
                      "     constraint check_id_ge_zero check (id >= 0)" +
                      ")")
            .build();
    }


    @Test
    public void testDropCheckConstraint() {
        AnalyzedAlterTableDropCheckConstraint analysis = e.analyze("alter table t drop constraint check_qty_gt_zero");
        assertThat(analysis.name()).isEqualTo("check_qty_gt_zero");
        assertThat(analysis.tableInfo().ident().name()).isEqualTo("t");
    }

    @Test
    public void testDropCheckConstraintFailsBecauseTheNameDoesNotReferToAnExistingConstraint() {
        Assertions.assertThatThrownBy(() -> e.analyze("alter table t drop constraint bazinga"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                    "Cannot find a CHECK CONSTRAINT named [bazinga], available constraints are: [check_qty_gt_zero, check_id_ge_zero]");
    }
}
