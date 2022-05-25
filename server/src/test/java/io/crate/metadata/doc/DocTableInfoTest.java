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

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.Asserts;
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
                new SimpleReference(
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
        final ColumnIdent col = new ColumnIdent("o", List.of("foobar"));
        SimpleReference foobar = info.getReference(col);
        assertNull(foobar);

        // forWrite: false, errorOnUnknownObjectKey: true, parentPolicy: dynamic
        DynamicReference reference = info.getDynamic(col, false, true);
        assertNull(reference);

        // forWrite: true, errorOnUnknownObjectKey: true, parentPolicy: dynamic
        reference = info.getDynamic(col, true, true);
        assertNotNull(reference);
        assertSame(reference.valueType(), DataTypes.UNDEFINED);

        // forWrite: true, errorOnUnknownObjectKey: false, parentPolicy: dynamic
        reference = info.getDynamic(col, true, false);
        assertNotNull(reference);
        assertSame(reference.valueType(), DataTypes.UNDEFINED);

        // forWrite: false, errorOnUnknownObjectKey: false, parentPolicy: dynamic
        reference = info.getDynamic(col, false, false);
        assertNotNull(reference);
        assertTrue(reference instanceof VoidReference);
        assertSame(reference.valueType(), DataTypes.UNDEFINED);
    }

    @Test
    public void testGetColumnInfoStrictParent() throws Exception {
        RelationName dummy = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, new ColumnIdent("foobar"));
        SimpleReference strictParent = new SimpleReference(
            foobarIdent,
            RowGranularity.DOC,
            DataTypes.UNTYPED_OBJECT,
            ColumnPolicy.STRICT,
            IndexType.PLAIN,
            true,
            false,
            1,
            null
        );

        Map<ColumnIdent, SimpleReference> references = Map.of(new ColumnIdent("foobar"), strictParent);

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

        final ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReference(columnIdent));

        // forWrite: false, errorOnUnknownObjectKey: true, parentPolicy: strict
        assertNull(info.getDynamic(columnIdent, false, true));

        // forWrite: true, errorOnUnknownObjectKey: true, parentPolicy: strict
        Asserts.assertThrowsMatches(
            () -> info.getDynamic(columnIdent, true, true),
            ColumnUnknownException.class,
            "Column foobar['foo']['bar'] unknown"
        );

        // forWrite: false, errorOnUnknownObjectKey: false, parentPolicy: strict
        assertNull(info.getDynamic(columnIdent, false, false));

        // forWrite: true, errorOnUnknownObjectKey: false, parentPolicy: strict
        Asserts.assertThrowsMatches(
            () -> assertNull(info.getDynamic(columnIdent, true, false)),
            ColumnUnknownException.class,
            "Column foobar['foo']['bar'] unknown"
        );

        final ColumnIdent columnIdent2 = new ColumnIdent("foobar", Collections.singletonList("foo"));
        assertNull(info.getReference(columnIdent2));

        // forWrite: false, errorOnUnknownObjectKey: true, parentPolicy: strict
        assertNull(info.getDynamic(columnIdent2, false, true));

        // forWrite: true, errorOnUnknownObjectKey: true, parentPolicy: strict
        Asserts.assertThrowsMatches(
            () -> assertNull(info.getDynamic(columnIdent2, true, true)),
            ColumnUnknownException.class,
            "Column foobar['foo'] unknown"
        );

        // forWrite: false, errorOnUnknownObjectKey: false, parentPolicy: strict
        assertNull(info.getDynamic(columnIdent2, false, false));

        // forWrite: true, errorOnUnknownObjectKey: false, parentPolicy: strict
        Asserts.assertThrowsMatches(
            () -> assertNull(info.getDynamic(columnIdent2, true, false)),
            ColumnUnknownException.class,
            "Column foobar['foo'] unknown"
        );

        SimpleReference colInfo = info.getReference(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
