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

package io.crate.analyze.relations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;


public class DocTableRelationTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testUpdatingPrimaryKeyThrowsCorrectException() {
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "t1"),
            "create table doc.t1 (i int primary key)",
            clusterService);
        DocTableRelation rel = new DocTableRelation(tableInfo);

        assertThatThrownBy(() -> rel.ensureColumnCanBeUpdated(ColumnIdent.of("i")))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for i: Updating a primary key is not supported");
    }

    @Test
    public void testUpdatingCompoundPrimaryKeyThrowsCorrectException() {
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "t1"),
            "create table doc.t1 (i int, j int, primary key (i, j))",
            clusterService);
        DocTableRelation rel = new DocTableRelation(tableInfo);

        assertThatThrownBy(() -> rel.ensureColumnCanBeUpdated(ColumnIdent.of("i")))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for i: Updating a primary key is not supported");
    }

    @Test
    public void testUpdatingClusteredByColumnThrowsCorrectException() {
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "t1"),
            "create table doc.t1 (i int) clustered by (i)",
            clusterService);
        DocTableRelation rel = new DocTableRelation(tableInfo);

        assertThatThrownBy(() -> rel.ensureColumnCanBeUpdated(ColumnIdent.of("i")))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for i: Updating a clustered-by column is not supported");
    }
}
