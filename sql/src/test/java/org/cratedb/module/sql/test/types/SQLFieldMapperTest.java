/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.module.sql.test.types;

import org.cratedb.Constants;
import org.cratedb.SQLCrateNodesTest;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.sql.ValidationException;
import org.cratedb.sql.types.SQLFieldMapper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SQLFieldMapperTest extends SQLCrateNodesTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected SQLFieldMapper mapper;

    private IndexMetaData getMetaData() throws IOException {
        Map<String, Object> mapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("string_field", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "not_analyzed");
                }});
                put("boolean_field", new HashMap<String, Object>(){{
                    put("type", "boolean");
                    put("index", "not_analyzed");
                }});
                put("byte_field", new HashMap<String, Object>(){{
                    put("type", "byte");
                    put("index", "not_analyzed");
                }});
                put("short_field", new HashMap<String, Object>(){{
                    put("type", "short");
                    put("index", "not_analyzed");
                }});
                put("integer_field", new HashMap<String, Object>(){{
                    put("type", "integer");
                    put("index", "not_analyzed");
                }});
                put("long_field", new HashMap<String, Object>(){{
                    put("type", "long");
                    put("index", "not_analyzed");
                }});
                put("float_field", new HashMap<String, Object>(){{
                    put("type", "float");
                    put("index", "not_analyzed");
                }});
                put("double_field", new HashMap<String, Object>(){{
                    put("type", "double");
                    put("index", "not_analyzed");
                }});
                put("date_field", new HashMap<String, Object>(){{
                    put("type", "date");
                    put("index", "not_analyzed");
                }});
                put("object_field", new HashMap<String, Object>(){{
                    put("type", "object");
                    put("properties", new HashMap<String, Object>(){{
                        put("title", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                        }});
                        put("size", new HashMap<String, Object>(){{
                            put("type", "short");
                            put("index", "not_analyzed");
                        }});
                        put("created", new HashMap<String, Object>(){{
                            put("type", "date");
                            put("index", "not_analyzed");
                        }});
                    }});
                }});
                put("ip_field", new HashMap<String, Object>() {{
                    put("type", "ip");
                    put("index", "not_analyzed");
                }});
            }});

        }};
        MappingMetaData mappingMetaData = new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE,
                mapping);
        return IndexMetaData.builder("test1")
                .numberOfReplicas(0)
                .numberOfShards(2)
                .putMapping(mappingMetaData)
                .build();
    }

    @Before
    public void beforeFieldMapperTest() throws IOException {
        IndexMetaData metaData = getMetaData();
        this.mapper = new SQLFieldMapper(
                SQL_TYPES,
                new IndexMetaDataExtractor(metaData));
    }

    @Test
    public void testBuiltinTypes() {
        assertEquals(100, this.mapper.mappedValue("byte_field", 100L));
        assertEquals(new Integer(100).shortValue(), this.mapper.mappedValue("short_field", 100L));
        assertEquals(100, this.mapper.mappedValue("integer_field", 100L));
        assertEquals(new Long(100L), this.mapper.mappedValue("long_field", 100));
        assertEquals(new Float(100.0), this.mapper.mappedValue("float_field", 100.0));
        assertEquals(new Double(10000.0), this.mapper.mappedValue("double_field",
                10000.0));
        assertEquals("value", this.mapper.mappedValue("string_field", "value"));
        assertEquals(true, this.mapper.mappedValue("boolean_field", true));
        assertEquals("127.0.0.1", this.mapper.mappedValue("ip_field", "127.0.0.1"));
    }

    @Test
    public void testDateType() {
        String[] fields = new String[]{"date_field", "object_field.created"};
        for (int i=0; i<2; i++) {
            assertEquals(0L, this.mapper.mappedValue(fields[i],
                    "1970-01-01T00:00:00"));
            assertEquals(0L,  this.mapper.mappedValue(fields[i], "1970-01-01"));
            assertEquals(0L,  this.mapper.mappedValue(fields[i], "1970-01-01"));
            assertEquals(1384793745289L, this.mapper.mappedValue(fields[i],
                    "2013-11-18T16:55:45.289715"));
            assertEquals(1384790145289L, this.mapper.mappedValue(fields[i],
                    1384790145.289));
            assertEquals(0L, this.mapper.mappedValue(fields[i], 0L));
        }
    }

    @Test
    public void testMapobjectColumn() {
        Object mapped = this.mapper.mappedValue("object_field", new HashMap<String,
                Object>() {{
            put("title", "The Total Perspective Vortex");
            put("size", 1024);
            put("created", "2013-11-18");
        }});
        assertTrue(mapped instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> mappedMap = (Map<String, Object>)mapped;
        assertEquals("The Total Perspective Vortex", mappedMap.get("title"));
        assertEquals(new Integer(1024).shortValue(), mappedMap.get("size"));
        assertEquals(1384732800000L, mappedMap.get("created"));

        assertEquals(1384732800000L, this.mapper.mappedValue("object_field.created",
                "2013-11-18"));
    }

    @Test
    public void testInvalidBoolean1() {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for boolean_field: Invalid boolean");
        this.mapper.mappedValue("boolean_field", 1);
    }

    @Test
    public void testInvalidBoolean2() {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for boolean_field: Invalid boolean");
        this.mapper.mappedValue("boolean_field", "A String");
    }

    @Test
    public void testInvalidBoolean3() {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for boolean_field: Invalid boolean");
        this.mapper.mappedValue("boolean_field", new HashMap<String, Object>());
    }
}
