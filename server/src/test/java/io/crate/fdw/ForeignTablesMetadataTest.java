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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class ForeignTablesMetadataTest extends ESTestCase {

    @Test
    public void test_xcontent_serialization() throws Exception {
        RelationName rel1 = new RelationName("s1", "t1");
        RelationName rel2 = new RelationName("s2", "t2");
        Map<String, Object> options1 = Map.of("schema_name", "public");
        Map<String, Object> options2 = Map.of("schema_name", "public", "table_name", "tbl2");
        Map<ColumnIdent, Reference> references1 = Map.of(
            new ColumnIdent("x"),
            new SimpleReference(
                new ReferenceIdent(rel1, "x"),
                RowGranularity.DOC,
                DataTypes.INTEGER,
                1,
                null
            )
        );
        ForeignTable table1 = new ForeignTable(rel1, references1, "pg1", options1);
        ForeignTable table2 = new ForeignTable(rel2, Map.of(), "pg1", options2);
        ForeignTablesMetadata foreignTables = new ForeignTablesMetadata(Map.of(
            rel1, table1,
            rel2, table2
        ));

        XContentBuilder builder = JsonXContent.builder();
        builder.startObject();
        foreignTables.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken();
        ForeignTablesMetadata deserializedTables = ForeignTablesMetadata.fromXContent(parser);

        assertThat(deserializedTables).isEqualTo(foreignTables);
        assertThat(parser.nextToken())
            .as("Must not have any left-over tokens")
            .isNull();
    }
}
