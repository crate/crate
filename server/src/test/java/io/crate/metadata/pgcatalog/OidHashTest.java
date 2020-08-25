/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.pgcatalog;

import io.crate.metadata.RelationInfo;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.crate.metadata.pgcatalog.OidHash.constraintOid;
import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.testing.T3.T1_DEFINITION;
import static io.crate.testing.T3.T1;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class OidHashTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationInfo VIEW_INFO =  new ViewInfo(T1, "", Collections.emptyList(), null);
    private RelationInfo t1Info;

    @Before
    public void prepare() throws Exception {
        t1Info = SQLExecutor.tableInfo(T1, T1_DEFINITION, clusterService);
    }

    @Test
    public void testRelationOid() {
        int tableOid = relationOid(t1Info);
        int viewOid = relationOid(VIEW_INFO);
        assertThat(tableOid, not(viewOid));
        assertThat(tableOid, is(728874843));
        assertThat(viewOid, is(1782608760));
    }

    @Test
    public void testSchemaOid() {
        assertThat(schemaOid(Schemas.DOC_SCHEMA_NAME), is(-2048275947));
    }

    @Test
    public void testConstraintOid() {
        assertThat(constraintOid(T1.fqn(), "id_pk", ConstraintInfo.Type.PRIMARY_KEY.toString()),
            is(279835673));
    }

    @Test
    public void test_argTypesToStr() {
        assertThat(OidHash
                       .argTypesToStr(List.of(
                           TypeSignature.parseTypeSignature("array(array(E))"),
                           TypeSignature.parseTypeSignature("array(Q)"),
                           TypeSignature.parseTypeSignature("P"),
                           DataTypes.INTEGER.getTypeSignature()
                       )), is("array_array_E array_Q P integer"));
    }
}
