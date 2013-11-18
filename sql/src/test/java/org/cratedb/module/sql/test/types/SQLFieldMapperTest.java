package org.cratedb.module.sql.test.types;

import org.cratedb.core.Constants;
import org.cratedb.core.IndexMetaDataExtractor;
import org.cratedb.sql.types.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class SQLFieldMapperTest {

    protected SQLFieldMapper mapper;

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
                put("craty_field", new HashMap<String, Object>(){{
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
                new HashMap<String, SQLType>() {{
                    put(BooleanSQLType.NAME, new BooleanSQLType());
                    put(ByteSQLType.NAME, new ByteSQLType());
                    put(ShortSQLType.NAME, new ShortSQLType());
                    put(IntegerSQLType.NAME, new IntegerSQLType());
                    put(LongSQLType.NAME, new LongSQLType());
                    put(FloatSQLType.NAME, new FloatSQLType());
                    put(DoubleSQLType.NAME, new DoubleSQLType());
                    put(StringSQLType.NAME, new StringSQLType());
                    put(CratySQLType.NAME, new CratySQLType());
                    put(TimeStampSQLType.NAME, new TimeStampSQLType());
                }},
                new IndexMetaDataExtractor(metaData));
    }

    @Test
    public void testBuiltinTypes() {
        assertEquals(100, this.mapper.convertToXContentValue("byte_field", 100L));
        assertEquals(100, this.mapper.convertToXContentValue("short_field", 100L));
        assertEquals(100, this.mapper.convertToXContentValue("integer_field", 100L));
        assertEquals(new Long(100L), this.mapper.convertToXContentValue("long_field", 100));
        assertEquals(new Float(100.0), this.mapper.convertToXContentValue("float_field", 100.0));
        assertEquals(new Double(10000.0), this.mapper.convertToXContentValue("double_field",
                10000.0));
        assertEquals("value", this.mapper.convertToXContentValue("string_field", "value"));
        assertEquals(true, this.mapper.convertToXContentValue("boolean_field", true));
    }

    @Test
    public void testDateType() {
        String[] fields = new String[]{"date_field", "craty_field.created"};
        for (int i=0; i<2; i++) {
            assertEquals(0L, this.mapper.convertToXContentValue(fields[i], "1970-01-01T01:00:00"));
            assertEquals(-3600000L,  this.mapper.convertToXContentValue(fields[i], "1970-01-01"));
            assertEquals(-3600000L,  this.mapper.convertToXContentValue(fields[i], "1970-01-01"));
            assertEquals(1384790145289L, this.mapper.convertToXContentValue(fields[i],
                    "2013-11-18T16:55:45.289715"));
            assertEquals(1384790145289L, this.mapper.convertToXContentValue(fields[i],
                    1384790145.289));
            assertEquals(0L, this.mapper.convertToXContentValue(fields[i], 0L));    
        }
        
    }
}
