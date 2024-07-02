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

package io.crate.metadata.pgcatalog;

import static io.crate.metadata.pgcatalog.OidHash.constraintOid;
import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.testing.T3.T1;
import static io.crate.testing.T3.T1_DEFINITION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.RelationInfo;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class OidHashTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationInfo VIEW_INFO = new ViewInfo(T1, "", Collections.emptyList(), null, SearchPath.pathWithPGCatalogAndDoc());
    private RelationInfo t1Info;

    @Before
    public void prepare() throws Exception {
        t1Info = SQLExecutor.tableInfo(T1, T1_DEFINITION, clusterService);
    }

    @Test
    public void testRelationOid() {
        int tableOid = relationOid(t1Info);
        int viewOid = relationOid(VIEW_INFO);
        assertThat(tableOid).isNotEqualTo(viewOid);
        assertThat(tableOid).isEqualTo(728874843);
        assertThat(viewOid).isEqualTo(1782608760);
    }

    @Test
    public void testSchemaOid() {
        assertThat(schemaOid(Schemas.DOC_SCHEMA_NAME)).isEqualTo(-2048275947);
    }

    @Test
    public void testConstraintOid() {
        assertThat(constraintOid(T1.fqn(), "id_pk", ConstraintInfo.Type.PRIMARY_KEY.toString())).isEqualTo(279835673);
    }

    @Test
    public void test_argTypesToStr() {
        assertThat(OidHash
                       .argTypesToStr(List.of(
                           TypeSignature.parse("array(array(E))"),
                           TypeSignature.parse("array(Q)"),
                           TypeSignature.parse("P"),
                           DataTypes.INTEGER.getTypeSignature()
                       ))).isEqualTo("array_array_E array_Q P integer");
    }
}
