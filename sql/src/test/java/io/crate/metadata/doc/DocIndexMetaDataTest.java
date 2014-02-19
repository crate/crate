package io.crate.metadata.doc;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.cratedb.index.IndexMetaDataExtractor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;

public class DocIndexMetaDataTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private IndexMetaData getIndexMetaData(String indexName, XContentBuilder builder) throws IOException {
        return getIndexMetaData(indexName, builder, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    private IndexMetaData getIndexMetaData(String indexName, XContentBuilder builder, Settings settings)
            throws IOException {
        byte[] data = builder.bytes().toBytes();
        Map<String, Object> mappingSource = XContentHelper.convertToMap(data, true).v2();
        mappingSource = sortProperties(mappingSource);

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(settings);

        return IndexMetaData.builder(indexName)
                .settings(settingsBuilder)
                .putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, mappingSource))
                .build();
    }

    private DocIndexMetaData newMeta(IndexMetaData metaData, String name) throws IOException {
        return new DocIndexMetaData(metaData, new TableIdent(null, name)).build();
    }

    @Test
    public void testNestedColumnIdent() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                    .startObject("person")
                        .startObject("properties")
                            .startObject("addresses")
                                .startObject("properties")
                                    .startObject("city")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("country")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject();

        IndexMetaData metaData = getIndexMetaData("test1", builder);
        DocIndexMetaData md = newMeta(metaData, "test1");

        ReferenceInfo referenceInfo = md.references().get(new ColumnIdent("person", Arrays.asList("addresses", "city")));
        assertNotNull(referenceInfo);
    }

    @Test
    public void testExtractObjectColumnDefinitions() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("implicit_dynamic")
                    .startObject("properties")
                        .startObject("name")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("explicit_dynamic")
                    .field("dynamic", "true")
                    .startObject("properties")
                        .startObject("name")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("age")
                            .field("type", "integer")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("ignored")
                    .field("dynamic", "false")
                    .startObject("properties")
                        .startObject("name")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("age")
                            .field("type", "integer")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("strict")
                    .field("dynamic", "strict")
                    .startObject("properties")
                        .startObject("age")
                            .field("type", "integer")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test1", builder);
        DocIndexMetaData md = newMeta(metaData, "test1");
        assertThat(md.columns().size(), is(4));
        assertThat(md.references().size(), is(13));
        assertThat(md.references().get(new ColumnIdent("implicit_dynamic")).objectType(), is(ReferenceInfo.ObjectType.DYNAMIC));
        assertThat(md.references().get(new ColumnIdent("explicit_dynamic")).objectType(), is(ReferenceInfo.ObjectType.DYNAMIC));
        assertThat(md.references().get(new ColumnIdent("ignored")).objectType(), is(ReferenceInfo.ObjectType.IGNORED));
        assertThat(md.references().get(new ColumnIdent("strict")).objectType(), is(ReferenceInfo.ObjectType.STRICT));
    }

    @Test
    public void testExtractColumnDefinitions() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_meta")
                .field("primary_keys", "id")
                .endObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("title")
                .field("type", "string")
                .field("index", "no")
                .endObject()
                .startObject("datum")
                .field("type", "date")
                .endObject()
                .startObject("content")
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "standard")
                .endObject()
                .startObject("person")
                .startObject("properties")
                .startObject("first_name")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("birthday")
                .field("type", "date")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .startObject("nested")
                .field("type", "nested")
                .startObject("properties")
                .startObject("inner_nested")
                .field("type", "date")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();


        IndexMetaData metaData = getIndexMetaData("test1", builder);
        DocIndexMetaData md = newMeta(metaData, "test1");

        assertEquals(6, md.columns().size());
        assertEquals(12, md.references().size());

        ImmutableList<ReferenceInfo> columns = ImmutableList.copyOf(md.columns());

        assertThat(columns.get(0).ident().columnIdent().name(), is("content"));
        assertThat(columns.get(0).type(), is(DataType.STRING));
        assertThat(columns.get(0).ident().tableIdent().name(), is("test1"));

        ImmutableList<ReferenceInfo> references = ImmutableList.<ReferenceInfo>copyOf(md.references().values());


        ReferenceInfo birthday = md.references().get(new ColumnIdent("person", "birthday"));
        assertThat(birthday.type(), is(DataType.TIMESTAMP));

        List<String> fqns = Lists.transform(references, new Function<ReferenceInfo, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ReferenceInfo input) {
                return input.ident().columnIdent().fqn();
            }
        });

        assertThat(fqns, Matchers.<List<String>>is(
                ImmutableList.of("content", "datum", "id", "nested", "nested.inner_nested",
                        "person", "person.birthday", "person.first_name", "title", "_id", "_version", "_score")));

    }

    private Map<String, Object> sortProperties(Map<String, Object> mappingSource) {
        return sortProperties(mappingSource, false);
    }

    /**
     * in the DocumentMapper that ES uses at some place the properties of the mapping are sorted.
     * this logic doesn't seem to be triggered if the IndexMetaData is created using the
     * IndexMetaData.Builder.
     * <p/>
     * in order to have the same behaviour as if a Node was started and a index with mapping was created
     * using the ES tools pre-sort the mapping here.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> sortProperties(Map<String, Object> mappingSource, boolean doSort) {
        Map<String, Object> map;
        if (doSort) {
            map = new TreeMap<>();
        } else {
            map = new HashMap<>();
        }

        boolean sortNext;
        Object value;
        for (Map.Entry<String, Object> entry : mappingSource.entrySet()) {
            value = entry.getValue();
            sortNext = entry.getKey().equals("properties");

            if (value instanceof Map) {
                map.put(entry.getKey(), sortProperties((Map) entry.getValue(), sortNext));
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        return map;
    }

    @Test
    public void testExtractColumnDefinitionsFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test2", builder);
        DocIndexMetaData md = newMeta(metaData, "test2");
        assertThat(md.columns(), hasSize(0));
    }

    @Test
    public void testDocSysColumnReferences() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("content")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        DocIndexMetaData metaData = newMeta(getIndexMetaData("test", builder), "test");
        ReferenceInfo id = metaData.references().get(new ColumnIdent("_id"));
        assertNotNull(id);

        ReferenceInfo version = metaData.references().get(new ColumnIdent("_version"));
        assertNotNull(version);

        ReferenceInfo score = metaData.references().get(new ColumnIdent("_score"));
        assertNotNull(score);
    }

    @Test
    public void testExtractPrimaryKey() throws Exception {

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("_meta")
                .field("primary_keys", "id")
                .endObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("title")
                .field("type", "string")
                .field("index", "no")
                .endObject()
                .startObject("datum")
                .field("type", "date")
                .endObject()
                .startObject("content")
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "standard")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test3", builder);
        DocIndexMetaData md = newMeta(metaData, "test3");


        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey(), contains("id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("content")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        md = newMeta(getIndexMetaData("test4", builder), "test4");
        assertThat(md.primaryKey().size(), is(1)); // _id is always the fallback primary key

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
                .endObject();
        md = newMeta(getIndexMetaData("test5", builder), "test5");
        assertThat(md.primaryKey().size(), is(1));
    }

    @Test
    public void testGetIndices() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("_meta")
                .field("primary_keys", "id")
                .endObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("title")
                .field("type", "multi_field")
                .field("path", "just_name")
                .startObject("fields")
                .startObject("title")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("ft")
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "english")
                .endObject()
                .endObject()
                .endObject()
                .startObject("datum")
                .field("type", "date")
                .endObject()
                .startObject("content")
                .field("type", "multi_field")
                .field("path", "just_name")
                .startObject("fields")
                .startObject("content")
                .field("type", "string")
                .field("index", "no")
                .endObject()
                .startObject("ft")
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "english")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        IndexMetaData metaData = getIndexMetaData("test6", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();

        assertEquals(5, indices.size());

        assertThat(indices.get(0).indexName, is("ft"));
        assertThat(indices.get(0).tableName, is("test6"));
        assertThat(indices.get(0).method, is("fulltext"));
        assertThat(indices.get(0).getColumnsString(), is("content, title"));
        assertThat(indices.get(0).getPropertiesString(), is("analyzer=english"));
        assertThat(indices.get(0).getUid(), is("test6.ft"));

        assertThat(indices.get(1).indexName, is("datum"));
        assertThat(indices.get(1).tableName, is("test6"));
        assertThat(indices.get(1).method, is("plain"));
        assertThat(indices.get(1).getColumnsString(), is("datum"));
        assertThat(indices.get(1).getUid(), is("test6.datum"));

        assertThat(indices.get(2).indexName, is("id"));
        assertThat(indices.get(2).tableName, is("test6"));
        assertThat(indices.get(2).getColumnsString(), is("id"));
        assertThat(indices.get(2).method, is("plain"));

        assertThat(indices.get(3).indexName, is("title"));
        assertThat(indices.get(3).tableName, is("test6"));
        assertThat(indices.get(3).getColumnsString(), is("title"));
        assertThat(indices.get(3).method, is("plain"));

        // compound indices will be listed once for every column they index,
        // will be merged by indicesTable
        assertThat(indices.get(4).indexName, is("ft"));
    }

    @Test
    public void extractIndicesFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test7", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();
        assertEquals(0, indices.size());

    }

    @Test
    public void extractRoutingColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("_meta")
                .field("primary_keys", "id")
                .endObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("title")
                .field("type", "multi_field")
                .field("path", "just_name")
                .startObject("fields")
                .startObject("title")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("ft")
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "english")
                .endObject()
                .endObject()
                .endObject()
                .startObject("datum")
                .field("type", "date")
                .endObject()
                .startObject("content")
                .field("type", "multi_field")
                .field("path", "just_name")
                .startObject("fields")
                .startObject("content")
                .field("type", "string")
                .field("index", "no")
                .endObject()
                .startObject("ft")
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "english")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        DocIndexMetaData md = newMeta(getIndexMetaData("test8", builder), "test8");
        assertThat(md.routingCol(), is("id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("content")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        md = newMeta(getIndexMetaData("test9", builder), "test8");
        assertThat(md.routingCol(), is("_id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("_meta")
                .field("primary_keys", "id")
                .endObject()
                .startObject("_routing")
                .field("path", "id")
                .endObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("content")
                .field("type", "string")
                .field("index", "no")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        md = newMeta(getIndexMetaData("test10", builder), "test10");
        assertThat(md.routingCol(), is("id"));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
                .endObject();
        DocIndexMetaData md = newMeta(getIndexMetaData("test11", builder), "test11");
        assertThat(md.routingCol(), is("_id"));
    }


    @Test
    public void testIsDynamic() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test13", builder,
                ImmutableSettings.builder().put("index.mapper.dynamic", false).build());
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", true)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        metaData = getIndexMetaData("test13", builder,
                ImmutableSettings.builder().put("index.mapper.dynamic", false).build());
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", false)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        metaData = getIndexMetaData("test15", builder);
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        metaData = getIndexMetaData("test16", builder);
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        metaData = getIndexMetaData("test17", builder);
        extractor = new IndexMetaDataExtractor(metaData);
        assertTrue(extractor.isDynamic());

    }
}



