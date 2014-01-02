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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;

@RunWith( value = Parameterized.class)
public class SQLFieldRangeTest {

    protected SQLFieldMapper mapper;

    private String fieldName;
    private Number[] testNumbers;
    public Class<?> klass;

    public SQLFieldRangeTest(String fieldName, Number[] testNumbers, Class<?> klass) {
        this.fieldName = fieldName;
        this.testNumbers = testNumbers;
        this.klass = klass;
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
                    put("ip_field", new HashMap<String, Object>() {{
                        put("type", "ip");
                        put("index", "not_analyzed");
                    }});
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
                {"byte_field", new Number[]{Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0,
                    Byte.MAX_VALUE, Byte.MAX_VALUE + 1}, Integer.class},
                {"short_field", new Number[]{Short.MIN_VALUE - 1, Short.MIN_VALUE, 0,
                        Short.MAX_VALUE,
                        Short.MAX_VALUE + 1}, Short.class},
                {"integer_field", new Number[]{Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE, Integer.MAX_VALUE + 1L}, Integer.class}
        };
        return Arrays.asList(data);
    }

    @Test
    public void testRange() {
        try {
            this.mapper.mappedValue(fieldName, testNumbers[0]);
            fail("did not validate lower bound");
        } catch(ValidationException e) {}

        assertThat(this.mapper.mappedValue(fieldName, testNumbers[1]),
                instanceOf(this.klass));
        assertThat(
                this.mapper.mappedValue(fieldName, testNumbers[2]),
                instanceOf(this.klass));
        assertThat(this.mapper.mappedValue(fieldName,
                testNumbers[3]),
                instanceOf(this.klass));
        try {
            this.mapper.mappedValue(fieldName, testNumbers[4]);
            fail("did not validate upper bound");
        } catch(ValidationException e) {}
    }
}
