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

package io.crate.testing;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.junit.Test;

public class SQLExecutorTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testAddDuplicateTableThrowsException() throws Exception {
        expectedException.expect(RelationAlreadyExists.class);

        SQLExecutor.builder(clusterService)
            .addTable("create table foo (id int)")
            .addTable("create table foo (id text)")
            .build();
    }

    @Test
    public void testAddDuplicateTableOnSameClusterStateThrowsException() throws Exception {
        SQLExecutor.builder(clusterService)
            .addTable("create table foo (id text)")
            .build();

        expectedException.expect(RelationAlreadyExists.class);
        SQLExecutor.builder(clusterService)
            .addTable("create table foo (id int)")
            .build();
    }
}
