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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith( value = Parameterized.class)
public class SQLFieldInvalidTest {
    private String fieldName;
    private Object[] invalidValues;

    protected SQLFieldMapper mapper;

    public SQLFieldInvalidTest(String fieldName, Object[] invalidValues) {
        this.fieldName = fieldName;
        this.invalidValues = invalidValues;
    }

    @Before
    public void before() throws IOException {
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
        IndexMetaData metaData = IndexMetaData.builder("test1")
                .numberOfReplicas(0)
                .numberOfShards(2)
                .putMapping(mappingMetaData)
                .build();
        this.mapper = new SQLFieldMapper(
                SQLCrateNodesTest.SQL_TYPES,
                new IndexMetaDataExtractor(metaData));
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {
                        "boolean_field",
                        new Object[]{-1, "A String", 0, 199.0, new HashMap<String, Object>()}
                },
                {
                        "byte_field",
                        new Object[]{ Byte.MAX_VALUE+1, Byte.MIN_VALUE-1, "A String", 99999.0,
                                true, false}
                },
                {
                        "short_field",
                        new Object[]{Short.MAX_VALUE+1, Short.MIN_VALUE-1, "A String",
                                new HashMap<String, Object>(), true, false}
                },
                {
                        "integer_field",
                        new Object[]{Integer.MAX_VALUE+1L, Integer.MIN_VALUE-1L, "A String",
                                new HashMap<String, Object>(), true, false}
                },
                {
                        "long_field",
                        new Object[]{"A String", new HashMap<String, Object>(), true, false}
                },
                {
                        "float_field",
                        new Object[]{"A String", new HashMap<String, Object>(), true, false}
                },
                {
                        "double_field",
                        new Object[]{"A String", new HashMap<String, Object>(), true, false}
                },
                {
                        "string_field",
                        new Object[]{Integer.MAX_VALUE, 1.0, new HashMap<String, Object>(), true,
                                false}
                },
                {
                        "object_field",
                        new Object[]{Integer.MAX_VALUE, 1.0, "A String", true, false,
                            new HashMap<String, Object>(){{
                                put("title", 0);
                                put("size", Integer.MAX_VALUE);
                                put("created", true);
                            }}
                        }
                },
                {
                        "date_field",
                        new Object[]{"No Date", true, false, new HashMap<String, Object>()}
                },
                {
                        "ip_field",
                        new Object[]{"no ip", true, false, 1, -1, Long.MAX_VALUE, 99.9, new HashMap<String, Object>()}
                }

        };
        return Arrays.asList(data);
    }

    @Test
    public void testInvalidValues() {
        for (Object invalidValue : invalidValues) {
            try {
                this.mapper.mappedValue(fieldName, invalidValue);
                fail(String.format("Validation of %s for '%s' did not work", fieldName,
                        invalidValue.toString()));
            } catch(ValidationException e) {
                //
            }
        }
    }
}
