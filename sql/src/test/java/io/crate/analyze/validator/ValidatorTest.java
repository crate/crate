/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.validator;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValidatorTest {

    private static ReferenceInfos referenceInfos;

    private final TableIdent testTableIdent = new TableIdent(null, "test");
    private final ReferenceIdent testReferenceIdent = new ReferenceIdent(testTableIdent, "person");

    @Before
    @SuppressWarnings("unchecked")
    public void mockReferenceInfos() {
        referenceInfos = mock(ReferenceInfos.class);
        TableInfo testTableInfo = TestingTableInfo.builder(testTableIdent, RowGranularity.DOC, new Routing())
                .add("person", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
                .add("person", DataTypes.INTEGER, ImmutableList.of("age"))
                .add("person", DataTypes.OBJECT, ImmutableList.of("name"))
                .add("person", DataTypes.STRING, ImmutableList.of("name", "first_name"))
                .add("person", DataTypes.STRING, ImmutableList.of("name", "last_name"))
                .add("person", DataTypes.OBJECT, ImmutableList.of("address"))
                .add("person", DataTypes.STRING, ImmutableList.of("address", "street"))
                .add("person", DataTypes.INTEGER, ImmutableList.of("address", "building"))
                .add("person", DataTypes.BOOLEAN, ImmutableList.of("address", "isHomeless")).build();
        when(referenceInfos.getTableInfoUnsafe(testTableIdent)).thenReturn(testTableInfo);
        when(referenceInfos.getReferenceInfo(testReferenceIdent)).thenReturn(
                new ReferenceInfo(testReferenceIdent, RowGranularity.DOC, DataTypes.OBJECT)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRepairArgs() {

        Map<String, Object> person1 = new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("first_name", "John");
                put("last_name", "Doe");
            }});
            put("age", 25);
            put("address", new HashMap<String, Object>() {{
                put("street", "Broadway");
                put("building", 7);
                put("isHomeless", false);
            }});
        }};

        Map<String, Object> person2 = new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("first_name", "");
                put("last_name", 0);
            }});
            put("age", null);
            put("address", new HashMap<String, Object>() {{
                put("street", true);
                put("building", "0");
                put("isHomeless", true);
            }});
        }};


        Object[][] args = new Object[][]{
                {"41", "NameOne", "192.168.0.1", new String[]{"1", "2", "3"}, person1},
                {42, 52345, "192.168.0.1", new String[]{"1", "2", "3"}, person2},
                {42, "52345", 3232235521L, new String[]{"1", "2", "3"}, person2},
                {null, "52345", 3232235521L, new String[]{"1", "2", "3"}, person1}
        };

        Reference referenceType = new Reference(referenceInfos.getReferenceInfo(testReferenceIdent));

        ArrayList<Symbol> colType = new ArrayList<>();
        colType.add(new Value(IntegerType.INSTANCE));
        colType.add(new Value(StringType.INSTANCE));
        colType.add(new Value(IpType.INSTANCE));
        colType.add(new Value(new ArrayType(StringType.INSTANCE)));
        colType.add(referenceType);

        Validator validator = new Validator(referenceInfos);
        validator.repairArgs(args, colType);
        assertEquals(args[0][0], 41);
        assertEquals(args[1][1], new BytesRef("52345"));
        assertEquals(args[1][2], new BytesRef("192.168.0.1"));
        assertEquals(args[1][2], new BytesRef("192.168.0.1"));
        assertEquals(args[3][0], null);
        assertTrue(((HashMap) args[0][4]).get("age") instanceof Integer);
        assertTrue(((HashMap) ((HashMap) args[0][4]).get("name")).get("first_name") instanceof String);
        assertTrue(((HashMap) ((HashMap) args[0][4]).get("address")).get("isHomeless") instanceof Boolean);
        assertNull(((HashMap) args[1][4]).get("age"));
        assertTrue(((HashMap) ((HashMap) args[1][4]).get("name")).get("last_name") instanceof BytesRef);
        assertTrue(((HashMap) ((HashMap) args[1][4]).get("address")).get("street") instanceof BytesRef);
        assertTrue(((HashMap) ((HashMap) args[0][4]).get("address")).get("building") instanceof Integer);
    }
}