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

package io.crate.execution.ddl.tables;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

public class CreateTableRequestTest {

    @Test
    public void test_streaming() throws Exception {
        RelationName rel = RelationName.of(QualifiedName.of("t1"), Schemas.DOC_SCHEMA_NAME);

        Reference ref1 = new SimpleReference(
            new ReferenceIdent(rel, "part_col_1"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        Reference ref2 = new SimpleReference(
            new ReferenceIdent(rel, "part_col_2"),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            2,
            null
        );
        Reference ref3 = new SimpleReference(
            new ReferenceIdent(rel, "just_col"),
            RowGranularity.DOC,
            DataTypes.BYTE,
            3,
            null
        );
        Reference ref4 = new SimpleReference(
            new ReferenceIdent(rel, "some_routing_col"),
            RowGranularity.DOC,
            DataTypes.LONG,
            4,
            null
        );
        List<Reference> refs = List.of(ref1, ref2, ref3, ref4);
        List<String> partCol1 = List.of("part_col_1", DataTypes.esMappingNameFrom(DataTypes.STRING.id()));
        List<String> partCol2 = List.of("part_col_2", DataTypes.esMappingNameFrom(DataTypes.INTEGER.id()));
        List<List<String>> partitionedBy = List.of(partCol1, partCol2);

        CreateTableRequest request = new CreateTableRequest(
            rel,
            null,
            refs,
            IntArrayList.from(3),
            Map.of("check1", "just_col > 0"),
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_4_0).build(),
            "some_routing_col",
            ColumnPolicy.DYNAMIC,
            partitionedBy
        );

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        CreateTableRequest fromStream = new CreateTableRequest(out.bytes().streamInput());

        assertThat(fromStream.getTableName()).isEqualTo(request.getTableName());
        assertThat(fromStream.references()).isEqualTo(request.references());
        assertThat(fromStream.checkConstraints()).isEqualTo(request.checkConstraints());
        assertThat(fromStream.pKeyIndices()).isEqualTo(request.pKeyIndices());

        assertThat(fromStream.settings()).isEqualTo(request.settings());
        assertThat(fromStream.routingColumn()).isEqualTo(request.routingColumn());
        assertThat(fromStream.tableColumnPolicy()).isEqualTo(request.tableColumnPolicy());
        assertThat(fromStream.partitionedBy()).containsExactlyElementsOf(request.partitionedBy());
    }

    @Test
    public void test_streaming_pk_constraint_name() throws Exception {
        CreateTableRequest request = new CreateTableRequest(
            new RelationName(null, "dummy"),
            "constraint_1",
            List.of(),
            new IntArrayList(),
            Map.of(),
            Settings.EMPTY,
            null,
            ColumnPolicy.DYNAMIC,
            List.of()
        );

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        CreateTableRequest actual = new CreateTableRequest(out.bytes().streamInput());
        assertThat(actual.pkConstraintName()).isEqualTo("constraint_1");

        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_5_0);
        request.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_5_5_0);
        actual = new CreateTableRequest(in);
        assertThat(actual.pkConstraintName()).isNull();
    }
}
