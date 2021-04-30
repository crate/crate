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

package io.crate.metadata.doc;

import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ColumnPolicy;
import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DocTableInfoTest extends ESTestCase {

    @Test
    public void testGetColumnInfo() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        DocTableInfo info = new DocTableInfo(
            relationName,
            List.of(
                new Reference(
                    new ReferenceIdent(relationName, new ColumnIdent("o", List.of())),
                    RowGranularity.DOC,
                    DataTypes.UNTYPED_OBJECT,
                    1,
                    null
                )
            ),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            List.of(),
            List.of(),
            null,
            true,
            new String[0],
            new String[0],
            new IndexNameExpressionResolver(),
            5,
            "0",
            Settings.EMPTY,
            List.of(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );

        Reference foobar = info.getReference(new ColumnIdent("o", List.of("foobar")));
        assertNull(foobar);
        DynamicReference reference = info.getDynamic(new ColumnIdent("o", List.of("foobar")), false);
        assertNull(reference);
        reference = info.getDynamic(new ColumnIdent("o", List.of("foobar")), true);
        assertNotNull(reference);
        assertSame(reference.valueType(), DataTypes.UNDEFINED);
    }

    @Test
    public void testGetColumnInfoStrictParent() throws Exception {
        RelationName dummy = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, new ColumnIdent("foobar"));
        Reference strictParent = new Reference(
            foobarIdent,
            RowGranularity.DOC,
            DataTypes.UNTYPED_OBJECT,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true,
            1,
            null
        );

        Map<ColumnIdent, Reference> references = Map.of(new ColumnIdent("foobar"), strictParent);

        DocTableInfo info = new DocTableInfo(
            dummy,
            List.of(strictParent),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            references,
            Map.of(),
            List.of(),
            List.of(),
            null,
            true,
            new String[0],
            new String[0],
            new IndexNameExpressionResolver(),
            5,
            "0",
            Settings.EMPTY,
            List.of(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );


        ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        columnIdent = new ColumnIdent("foobar", Collections.singletonList("foo"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        Reference colInfo = info.getReference(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
