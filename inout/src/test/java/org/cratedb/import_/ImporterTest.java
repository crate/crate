package org.cratedb.import_;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ImporterTest {

    @Test
    public void testParseId() throws Exception {
        IndexRequest r = Importer.parseObject("{\"_id\":\"i\"}", "test",
                "default", null);
        assertEquals("i", r.id());
    }


    @Test
    public void testParseIdAfterSubObject() throws Exception {
        IndexRequest r = Importer.parseObject("{\"o\":{\"x\":1},\"_id\":\"i\"}", "test",
                "default", null);
        assertEquals("i", r.id());
    }

    @Test
    public void testParsePrimaryKey() throws Exception {
        IndexRequest r = Importer.parseObject("{\"o\":{\"x\":1},\"mykey\":\"i\"}", "test",
                "default", "mykey");
        assertEquals("i", r.id());
    }

    @Test
    public void testPreferIdOverPrimaryKey() throws Exception {
        IndexRequest r = Importer.parseObject("{\"mykey\":\"k\",\"_id\":\"i\"}", "test",
                "default", "mykey");
        assertEquals("i", r.id());
        r = Importer.parseObject("{\"_id\":\"i\",\"mykey\":\"k\"}", "test", "default", "mykey");
        assertEquals("i", r.id());
    }

    @Test
    public void testNestedIdNotPrimaryKey() throws Exception {
        String src = "{\"contributor\":{\"username\":\"Gtrmp\",\"id\":\"sub\"}," +
                "\"id\":\"top\"}";
        IndexRequest r = Importer.parseObject(src, "test", "default", "id");
        assertEquals("top", r.id());
    }




}
