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

package io.crate.metadata.sys;

import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SysClusterTableInfoTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_license_data_can_be_selected() {
        var clusterTable = SysClusterTableInfo.of(
            clusterService,
            new CrateSettings(clusterService, clusterService.getSettings()));

        StaticTableReferenceResolver<Void> refResolver = new StaticTableReferenceResolver<>(clusterTable.expressions());
        NestableCollectExpression<Void, ?> expiryDate = refResolver.getImplementation(clusterTable.getReference(new ColumnIdent(
            "license",
            "expiry_date")));
        expiryDate.setNextRow(null);
        assertThat(expiryDate.value(), Matchers.nullValue());

        NestableCollectExpression<Void, ?> issuedTo = refResolver.getImplementation(clusterTable.getReference(new ColumnIdent(
            "license",
            "issued_to")));
        issuedTo.setNextRow(null);
        assertThat(issuedTo.value(), Matchers.nullValue());

        NestableCollectExpression<Void, ?> maxNodes = refResolver.getImplementation(clusterTable.getReference(new ColumnIdent(
            "license",
            "max_nodes")));
        maxNodes.setNextRow(null);
        assertThat(maxNodes.value(), Matchers.nullValue());
    }
}
