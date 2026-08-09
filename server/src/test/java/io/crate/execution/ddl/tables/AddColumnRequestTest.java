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
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class AddColumnRequestTest {

    @Test
    public void test_streaming_table_oids() throws Exception {
        // streaming to nodes with supported versions
        RelationName relation = new RelationName("doc", "tbl");
        int tableOid = 1234;
        var ref = new SimpleReference(
            relation, ColumnIdent.of("x"), RowGranularity.DOC, DataTypes.INTEGER, 1, null);
        AddColumnRequest request = new AddColumnRequest(
            relation, tableOid, List.of(ref), Map.of(), new IntArrayList());

        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_5_0);
        request.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_5_0);
        AddColumnRequest streamed = new AddColumnRequest(in);

        assertThat(streamed.tableOid()).isEqualTo(tableOid);

        // streaming to nodes with unsupported versions
        relation = new RelationName("doc", "tbl");
        ref = new SimpleReference(
            relation, ColumnIdent.of("x"), RowGranularity.DOC, DataTypes.INTEGER, 1, null);
        request = new AddColumnRequest(
            relation, 1234, List.of(ref), Map.of(), new IntArrayList());

        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_4_0);
        request.writeTo(out);

        in = out.bytes().streamInput();
        in.setVersion(Version.V_6_4_0);
        streamed = new AddColumnRequest(in);

        assertThat(streamed.tableOid()).isEqualTo(OID_UNASSIGNED);
    }
}
