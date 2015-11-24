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
import io.crate.types.DataTypes;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class T3 {

    public static final DocTableInfo t1Info = new TestingTableInfo.Builder(new TableIdent(null, "t1"), null)
            .add("a", DataTypes.STRING)
            .add("x", DataTypes.INTEGER)
            .build();
    public static final DocTableRelation tr1 = new DocTableRelation(t1Info);

    public static final DocTableInfo t2Info = new TestingTableInfo.Builder(new TableIdent(null, "t2"), null)
            .add("b", DataTypes.STRING)
            .add("y", DataTypes.INTEGER)
            .build();
    public static final DocTableRelation tr2 = new DocTableRelation(t2Info);

    public static final TableInfo t3Info = new TestingTableInfo.Builder(new TableIdent(null, "t3"), null)
            .add("c", DataTypes.STRING)
            .add("z", DataTypes.INTEGER)
            .build();
    public static final TableRelation tr3 = new TableRelation(t3Info);

    public static final MetaDataModule META_DATA_MODULE = new MetaDataModule() {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(t1Info.ident().name())).thenReturn(t1Info);
            when(schemaInfo.getTableInfo(t2Info.ident().name())).thenReturn(t2Info);
            when(schemaInfo.name()).thenReturn(Schemas.DEFAULT_SCHEMA_NAME);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    };

    public static final ImmutableList<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(tr1, tr2, tr3);
}
