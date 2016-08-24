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
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class T3 {

    public static final DocTableInfo T1_INFO = new TestingTableInfo.Builder(new TableIdent(null, "t1"), null)
            .add("a", DataTypes.STRING)
            .add("x", DataTypes.INTEGER)
            .add("i", DataTypes.INTEGER)
            .build();
    public static final DocTableRelation TR_1 = new DocTableRelation(T1_INFO);

    public static final DocTableInfo T2_INFO = new TestingTableInfo.Builder(new TableIdent(null, "t2"), null)
            .add("b", DataTypes.STRING)
            .add("y", DataTypes.INTEGER)
            .add("i", DataTypes.INTEGER)
            .build();
    public static final DocTableRelation TR_2 = new DocTableRelation(T2_INFO);

    public static final TableInfo T3_INFO = new TestingTableInfo.Builder(new TableIdent(null, "t3"), null)
            .add("c", DataTypes.STRING)
            .add("z", DataTypes.INTEGER)
            .build();
    public static final TableRelation TR_3 = new TableRelation(T3_INFO);

    public static final MetaDataModule META_DATA_MODULE = new MetaDataModule() {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(T1_INFO.ident().name())).thenReturn(T1_INFO);
            when(schemaInfo.getTableInfo(T2_INFO.ident().name())).thenReturn(T2_INFO);
            when(schemaInfo.getTableInfo(T3_INFO.ident().name())).thenReturn(T3_INFO);
            when(schemaInfo.name()).thenReturn(Schemas.DEFAULT_SCHEMA_NAME);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    };

    public static final QualifiedName T1 = new QualifiedName(Arrays.asList(Schemas.DEFAULT_SCHEMA_NAME, "t1"));
    public static final QualifiedName T2 = new QualifiedName(Arrays.asList(Schemas.DEFAULT_SCHEMA_NAME, "t2"));
    public static final QualifiedName T3 = new QualifiedName(Arrays.asList(Schemas.DEFAULT_SCHEMA_NAME, "t3"));

    public static final ImmutableList<AnalyzedRelation> RELATIONS = ImmutableList.<AnalyzedRelation>of(TR_1, TR_2, TR_3);
    public static final Map<QualifiedName, AnalyzedRelation> SOURCES = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
            T1, TR_1,
            T2, TR_2,
            T3, TR_3
    );
}
