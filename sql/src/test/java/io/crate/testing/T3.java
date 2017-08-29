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

package io.crate.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class T3 {

    private static final Routing t1Routing = new Routing(
        ImmutableMap.of(CrateDummyClusterServiceUnitTest.NODE_ID,
            ImmutableMap.of("t1", Collections.singletonList(0))));

    private static final Routing t2Routing = new Routing(
        ImmutableMap.of(CrateDummyClusterServiceUnitTest.NODE_ID,
            ImmutableMap.of("t2", Arrays.asList(0, 1))));

    private static final Routing t3Routing = new Routing(
        ImmutableMap.of(CrateDummyClusterServiceUnitTest.NODE_ID,
            ImmutableMap.of("t3", Arrays.asList(0, 1, 2))));

    private static final Routing t4Routing = new Routing(
        ImmutableMap.of(CrateDummyClusterServiceUnitTest.NODE_ID,
            ImmutableMap.of("t4", Collections.singletonList(0))));

    public static final DocTableInfo T1_INFO =
        new TestingTableInfo.Builder(new TableIdent(Schemas.DOC_SCHEMA_NAME, "t1"), t1Routing)
        .add("a", DataTypes.STRING)
        .add("x", DataTypes.INTEGER)
        .add("i", DataTypes.INTEGER)
        .build();
    public static final DocTableRelation TR_1 = new DocTableRelation(T1_INFO);

    public static final DocTableInfo T2_INFO = new TestingTableInfo.Builder(
        new TableIdent(Schemas.DOC_SCHEMA_NAME, "t2"), t2Routing)
        .add("b", DataTypes.STRING)
        .add("y", DataTypes.INTEGER)
        .add("i", DataTypes.INTEGER)
        .build();
    public static final DocTableRelation TR_2 = new DocTableRelation(T2_INFO);

    public static final DocTableInfo T3_INFO = new TestingTableInfo.Builder(
        new TableIdent(Schemas.DOC_SCHEMA_NAME, "t3"), t3Routing)
        .add("c", DataTypes.STRING)
        .add("z", DataTypes.INTEGER)
        .build();
    public static final TableRelation TR_3 = new TableRelation(T3_INFO);

    public static final DocTableInfo T4_INFO = new TestingTableInfo.Builder(
        new TableIdent(Schemas.DOC_SCHEMA_NAME, "t4"), t4Routing)
        .add("id", DataTypes.INTEGER)
        .add("obj", DataTypes.OBJECT)
        .add("obj", DataTypes.INTEGER, ImmutableList.of("i"))
        .add("obj_array", new ArrayType(DataTypes.OBJECT))
        .add("obj_array", DataTypes.INTEGER, ImmutableList.of("i"))
        .build();

    public static final TableRelation TR_4 = new TableRelation(T4_INFO);

    public static final QualifiedName T1 = new QualifiedName(Arrays.asList(Schemas.DOC_SCHEMA_NAME, "t1"));
    public static final QualifiedName T2 = new QualifiedName(Arrays.asList(Schemas.DOC_SCHEMA_NAME, "t2"));
    public static final QualifiedName T3 = new QualifiedName(Arrays.asList(Schemas.DOC_SCHEMA_NAME, "t3"));
    public static final QualifiedName T4 = new QualifiedName(Arrays.asList(Schemas.DOC_SCHEMA_NAME, "t4"));

    public static final ImmutableList<AnalyzedRelation> RELATIONS = ImmutableList.of(TR_1, TR_2, TR_3, TR_4);
    public static final Map<QualifiedName, AnalyzedRelation> SOURCES = ImmutableMap.of(
        T1, TR_1,
        T2, TR_2,
        T3, TR_3,
        T4, TR_4);
}
