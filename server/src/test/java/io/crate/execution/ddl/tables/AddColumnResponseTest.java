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

import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.Schemas;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AddColumnResponseTest {

    @Test
    public void test_bwc_streaming() throws Exception {
        RelationName rel = RelationName.of(QualifiedName.of("t1"), Schemas.DOC_SCHEMA_NAME);

        Reference ref1 = new SimpleReference(
            new ReferenceIdent(rel, "col_1"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        Reference ref2 = new SimpleReference(
            new ReferenceIdent(rel, "col_2"),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            2,
            null
        );
        List<Reference> refs = List.of(ref1, ref2);

        AddColumnResponse response = new AddColumnResponse(true, refs);

        // 5.5 -> 5.4
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_4_0);
            response.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            // 5.5 sends AddColumnResponse and 5.4 can read it as an AcknowledgedResponse.
            AcknowledgedResponse fromStream = new AcknowledgedResponse(in);
            assertThat(fromStream.isAcknowledged()).isTrue();
        }

        // 5.4 -> 5.5
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            AcknowledgedResponse acknowledgedResponse = new AcknowledgedResponse(true);
            acknowledgedResponse.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_5_4_0);
            // 5.4 sends AcknowledgedResponse and 5.5 can read it as an AddColumnResponse with empty columns list.
            AddColumnResponse fromStream = new AddColumnResponse(in);
            assertThat(fromStream.isAcknowledged()).isTrue();
            assertThat(fromStream.addedColumns()).isEmpty();
        }

        // 5.5 -> 5.5
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_5_0);
            response.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_5_5_0);
            AddColumnResponse fromStream = new AddColumnResponse(in);
            assertThat(fromStream.isAcknowledged()).isTrue();
            assertThat(fromStream.addedColumns()).isEqualTo(refs);
        }
    }

}
