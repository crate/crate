package io.crate.metadata.doc;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.*;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.SymbolMatchers.*;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.Mockito.mock;

public class DocIndexMetaDataTest extends CrateUnitTest {

    private Functions functions;
    private ExecutorService executorService;

    private IndexMetaData getIndexMetaData(String indexName, XContentBuilder builder) throws IOException {
        return getIndexMetaData(indexName, builder, Settings.Builder.EMPTY_SETTINGS, null);
    }

    private IndexMetaData getIndexMetaData(String indexName,
                                           XContentBuilder builder,
                                           Settings settings,
                                           @Nullable AliasMetaData aliasMetaData) throws IOException {
        Map<String, Object> mappingSource = XContentHelper.convertToMap(builder.bytes(), true).v2();
        mappingSource = sortProperties(mappingSource);

        Settings.Builder settingsBuilder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put(settings);

        IndexMetaData.Builder mdBuilder = IndexMetaData.builder(indexName)
            .settings(settingsBuilder)
            .putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, mappingSource));
        if (aliasMetaData != null) {
            mdBuilder.putAlias(aliasMetaData);
        }
        return mdBuilder.build();
    }

    private DocIndexMetaData newMeta(IndexMetaData metaData, String name) throws IOException {
        return new DocIndexMetaData(functions, metaData, new TableIdent(null, name)).build();
    }

    @Before
    public void before() throws Exception {
        executorService = Executors.newFixedThreadPool(1);
        functions = getFunctions();
    }

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
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

        Reference reference = md.references().get(new ColumnIdent("person", Arrays.asList("addresses", "city")));
        assertNotNull(reference);
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
        assertThat(md.references().size(), is(17));
        assertThat(md.references().get(new ColumnIdent("implicit_dynamic")).columnPolicy(), is(ColumnPolicy.DYNAMIC));
        assertThat(md.references().get(new ColumnIdent("explicit_dynamic")).columnPolicy(), is(ColumnPolicy.DYNAMIC));
        assertThat(md.references().get(new ColumnIdent("ignored")).columnPolicy(), is(ColumnPolicy.IGNORED));
        assertThat(md.references().get(new ColumnIdent("strict")).columnPolicy(), is(ColumnPolicy.STRICT));
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
        assertEquals(16, md.references().size());

        ImmutableList<Reference> columns = ImmutableList.copyOf(md.columns());

        assertThat(columns.get(0).ident().columnIdent().name(), is("content"));
        assertEquals(DataTypes.STRING, columns.get(0).valueType());
        assertEquals(Reference.IndexType.ANALYZED, columns.get(0).indexType());
        assertThat(columns.get(0).ident().tableIdent().name(), is("test1"));

        ImmutableList<Reference> references = ImmutableList.<Reference>copyOf(md.references().values());


        Reference birthday = md.references().get(new ColumnIdent("person", "birthday"));
        assertEquals(DataTypes.TIMESTAMP, birthday.valueType());
        assertEquals(Reference.IndexType.NOT_ANALYZED, birthday.indexType());

        Reference title = md.references().get(new ColumnIdent("title"));
        assertEquals(Reference.IndexType.NO, title.indexType());

        List<String> fqns = Lists.transform(references, new Function<Reference, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Reference input) {
                return input.ident().columnIdent().fqn();
            }
        });

        assertThat(fqns, Matchers.<List<String>>is(
            ImmutableList.of("_doc", "_fetchid", "_id", "_raw", "_score", "_uid", "_version", "content", "datum", "id", "nested", "nested.inner_nested",
                "person", "person.birthday", "person.first_name", "title")));

    }

    @Test
    public void testExtractPartitionedByColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .field("primary_keys", "id")
            .startArray("partitioned_by")
            .startArray()
            .value("datum").value("date")
            .endArray()
            .endArray()
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
        assertEquals(16, md.references().size());
        assertEquals(1, md.partitionedByColumns().size());
        assertEquals(DataTypes.TIMESTAMP, md.partitionedByColumns().get(0).valueType());
        assertThat(md.partitionedByColumns().get(0).ident().columnIdent().fqn(), is("datum"));

        assertThat(md.partitionedBy().size(), is(1));
        assertThat(md.partitionedBy().get(0), is(ColumnIdent.fromPath("datum")));
    }

    @Test
    public void testExtractPartitionedByWithPartitionedByInColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .startArray("partitioned_by")
            .startArray()
            .value("datum").value("date")
            .endArray()
            .endArray()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("index", "not_analyzed")
            .endObject()
            .startObject("datum")
            .field("type", "date")
            .field("index", "not_analyzed")
            .endObject()
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test1", builder);
        DocIndexMetaData md = newMeta(metaData, "test1");

        // partitioned by column is not added twice
        assertEquals(2, md.columns().size());
        assertEquals(9, md.references().size());
        assertEquals(1, md.partitionedByColumns().size());
    }

    @Test
    public void testExtractPartitionedByWithNestedPartitionedByInColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .startArray("partitioned_by")
            .startArray()
            .value("nested.datum").value("date")
            .endArray()
            .endArray()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("index", "not_analyzed")
            .endObject()
            .startObject("nested")
            .field("type", "nested")
            .startObject("properties")
            .startObject("datum")
            .field("type", "date")
            .field("index", "not_analyzed")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test1", builder);
        DocIndexMetaData md = newMeta(metaData, "test1");

        // partitioned by column is not added twice
        assertEquals(2, md.columns().size());
        assertEquals(10, md.references().size());
        assertEquals(1, md.partitionedByColumns().size());
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
        Reference id = metaData.references().get(new ColumnIdent("_id"));
        assertNotNull(id);

        Reference version = metaData.references().get(new ColumnIdent("_version"));
        assertNotNull(version);

        Reference score = metaData.references().get(new ColumnIdent("_score"));
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
        assertThat(md.primaryKey(), contains(new ColumnIdent("id")));

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
    public void testExtractMultiplePrimaryKeys() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .array("primary_keys", "id", "title")
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
            .endObject()
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test_multi_pk", builder);
        DocIndexMetaData md = newMeta(metaData, "test_multi_pk");
        assertThat(md.primaryKey().size(), is(2));
        assertThat(md.primaryKey(), hasItems(ColumnIdent.fromPath("id"), ColumnIdent.fromPath("title")));
    }

    @Test
    public void testExtractNoPrimaryKey() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
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
            .endObject()
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test_no_pk", builder);
        DocIndexMetaData md = newMeta(metaData, "test_no_pk");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey(), hasItems(ColumnIdent.fromPath("_id")));


        builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .array("primary_keys") // results in empty list
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
            .endObject()
            .endObject()
            .endObject();
        metaData = getIndexMetaData("test_no_pk2", builder);
        md = newMeta(metaData, "test_no_pk2");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey(), hasItems(ColumnIdent.fromPath("_id")));
    }

    @Test
    public void testSchemaWithNotNullColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .startObject("constraints")
            .array("not_null", "id", "title")
            .endObject()
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
            .endObject()
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test_notnull_columns", builder);
        DocIndexMetaData md = newMeta(metaData, "test_notnull_columns");
        assertThat(md.columns().get(0).isNullable(), is(false));
        assertThat(md.columns().get(1).isNullable(), is(false));
    }

    @Test
    public void testSchemaWithNotNullGeneratedColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .startObject("generated_columns")
            .field("week", "date_trunc('week', ts)")
            .endObject()
            .startObject("constraints")
            .array("not_null", "week")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("ts").field("type", "date").endObject()
            .startObject("week").field("type", "long").endObject()
            .endObject();

        IndexMetaData metaData = getIndexMetaData("test1", builder, Settings.EMPTY, null);
        DocIndexMetaData md = newMeta(metaData, "test1");

        assertThat(md.columns().size(), is(2));
        Reference week = md.references().get(new ColumnIdent("week"));
        assertThat(week, Matchers.notNullValue());
        assertThat(week.isNullable(), is(false));
        assertThat(week, instanceOf(GeneratedReference.class));
        assertThat(((GeneratedReference) week).formattedGeneratedExpression(), is("date_trunc('week', ts)"));
        assertThat(((GeneratedReference) week).generatedExpression(), isFunction("date_trunc", isLiteral("week"), isReference("ts")));
        assertThat(((GeneratedReference) week).referencedReferences(), contains(isReference("ts")));
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
        assertThat(md.routingCol(), is(new ColumnIdent("id")));

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
        assertThat(md.routingCol(), is(new ColumnIdent("_id")));

        builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .array("primary_keys", "id", "num")
            .field("routing", "num")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("index", "not_analyzed")
            .endObject()
            .startObject("num")
            .field("type", "long")
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
        assertThat(md.routingCol(), is(new ColumnIdent("num")));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        DocIndexMetaData md = newMeta(getIndexMetaData("test11", builder), "test11");
        assertThat(md.routingCol(), is(new ColumnIdent("_id")));
    }

    @Test
    public void testAutogeneratedPrimaryKey() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        DocIndexMetaData md = newMeta(getIndexMetaData("test11", builder), "test11");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey().get(0), is(new ColumnIdent("_id")));
        assertThat(md.hasAutoGeneratedPrimaryKey(), is(true));
    }

    @Test
    public void testNoAutogeneratedPrimaryKey() throws Exception {
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
            .endObject()
            .endObject();
        DocIndexMetaData md = newMeta(getIndexMetaData("test11", builder), "test11");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey().get(0), is(new ColumnIdent("id")));
        assertThat(md.hasAutoGeneratedPrimaryKey(), is(false));
    }

    @Test
    public void testAnalyzedColumnWithAnalyzer() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("properties")
            .startObject("content_de")
            .field("type", "string")
            .field("index", "analyzed")
            .field("analyzer", "german")
            .endObject()
            .startObject("content_en")
            .field("type", "string")
            .field("analyzer", "english")
            .endObject()
            .endObject()
            .endObject();
        DocIndexMetaData md = newMeta(getIndexMetaData("test_analyzer", builder), "test_analyzer");
        assertThat(md.columns().size(), is(2));
        assertThat(md.columns().get(0).indexType(), is(Reference.IndexType.ANALYZED));
        assertThat(md.columns().get(0).ident().columnIdent().fqn(), is("content_de"));
        assertThat(md.columns().get(1).indexType(), is(Reference.IndexType.ANALYZED));
        assertThat(md.columns().get(1).ident().columnIdent().fqn(), is("content_en"));
    }

    @Test
    public void testGeoPointType() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table foo (p geo_point)");
        assertThat(md.columns().size(), is(1));
        Reference reference = md.columns().get(0);
        assertThat((GeoPointType) reference.valueType(), equalTo(DataTypes.GEO_POINT));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingCompat() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table foo (" +
                                                               "id int primary key," +
                                                               "tags array(string)," +
                                                               "o object as (" +
                                                               "   age int," +
                                                               "   name string" +
                                                               ")," +
                                                               "date timestamp primary key" +
                                                               ") partitioned by (date)");

        assertThat(md.columns().size(), is(4));
        assertThat(md.primaryKey(), Matchers.contains(new ColumnIdent("id"), new ColumnIdent("date")));
        assertThat(md.references().get(new ColumnIdent("tags")).valueType(), is((DataType) new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingArrayInsideObject() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement(
            "create table t1 (" +
            "id int primary key," +
            "details object as (names array(string))" +
            ") with (number_of_replicas=0)");
        DataType type = md.references().get(new ColumnIdent("details", "names")).valueType();
        assertThat(type, Matchers.<DataType>equalTo(new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingCompatNoMeta() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table foo (id int, name string)");
        assertThat(md.columns().size(), is(2));
        assertThat(md.hasAutoGeneratedPrimaryKey(), is(true));
    }

    private DocIndexMetaData getDocIndexMetaDataFromStatement(String stmt) throws IOException {
        Statement statement = SqlParser.createStatement(stmt);

        ClusterService clusterService = new NoopClusterService();
        final TransportPutIndexTemplateAction transportPutIndexTemplateAction = mock(TransportPutIndexTemplateAction.class);
        Provider<TransportPutIndexTemplateAction> indexTemplateActionProvider = new Provider<TransportPutIndexTemplateAction>() {
            @Override
            public TransportPutIndexTemplateAction get() {
                return transportPutIndexTemplateAction;
            }
        };
        DocTableInfoFactory docTableInfoFactory = new InternalDocTableInfoFactory(
            functions,
            new IndexNameExpressionResolver(Settings.EMPTY),
            indexTemplateActionProvider,
            executorService
        );
        DocSchemaInfo docSchemaInfo = new DocSchemaInfo(Schemas.DEFAULT_SCHEMA_NAME, clusterService, docTableInfoFactory);
        CreateTableStatementAnalyzer analyzer = new CreateTableStatementAnalyzer(
            new Schemas(
                Settings.EMPTY,
                ImmutableMap.<String, SchemaInfo>of("doc", docSchemaInfo),
                clusterService,
                new DocSchemaInfoFactory(docTableInfoFactory)),
            new FulltextAnalyzerResolver(clusterService, new IndicesAnalysisService(Settings.EMPTY)),
            functions,
            new NumberOfShards(clusterService)
        );

        Analysis analysis = new Analysis(SessionContext.SYSTEM_SESSION, ParameterContext.EMPTY, ParamTypeHints.EMPTY);
        CreateTableAnalyzedStatement analyzedStatement = analyzer.analyze(statement, analysis);

        Settings.Builder settingsBuilder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put(analyzedStatement.tableParameter().settings());

        IndexMetaData indexMetaData = IndexMetaData.builder(analyzedStatement.tableIdent().name())
            .settings(settingsBuilder)
            .putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, analyzedStatement.mapping()))
            .build();

        return newMeta(indexMetaData, analyzedStatement.tableIdent().name());
    }

    @Test
    public void testCompoundIndexColumn() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  name string," +
                                                               "  fun string index off," +
                                                               "  INDEX fun_name_ft using fulltext(name, fun)" +
                                                               ")");
        assertThat(md.indices().size(), is(1));
        assertThat(md.columns().size(), is(3));
        assertThat(md.indices().get(ColumnIdent.fromPath("fun_name_ft")), instanceOf(IndexReference.class));
        IndexReference indexInfo = md.indices().get(ColumnIdent.fromPath("fun_name_ft"));
        assertThat(indexInfo.indexType(), is(Reference.IndexType.ANALYZED));
        assertThat(indexInfo.ident().columnIdent().fqn(), is("fun_name_ft"));
    }

    @Test
    public void testCompoundIndexColumnNested() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  name string," +
                                                               "  o object as (" +
                                                               "    fun string" +
                                                               "  )," +
                                                               "  INDEX fun_name_ft using fulltext(name, o['fun'])" +
                                                               ")");
        assertThat(md.indices().size(), is(1));
        assertThat(md.columns().size(), is(3));
        assertThat(md.indices().get(ColumnIdent.fromPath("fun_name_ft")), instanceOf(IndexReference.class));
        IndexReference indexInfo = md.indices().get(ColumnIdent.fromPath("fun_name_ft"));
        assertThat(indexInfo.indexType(), is(Reference.IndexType.ANALYZED));
        assertThat(indexInfo.ident().columnIdent().fqn(), is("fun_name_ft"));
    }

    @Test
    public void testExtractColumnPolicy() throws Exception {
        XContentBuilder ignoredBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", false)
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
        DocIndexMetaData mdIgnored = newMeta(getIndexMetaData("test_ignored", ignoredBuilder), "test_ignored");
        assertThat(mdIgnored.columnPolicy(), is(ColumnPolicy.IGNORED));

        XContentBuilder strictBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", "strict")
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
        DocIndexMetaData mdStrict = newMeta(getIndexMetaData("test_strict", strictBuilder), "test_strict");
        assertThat(mdStrict.columnPolicy(), is(ColumnPolicy.STRICT));

        XContentBuilder dynamicBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", true)
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
        DocIndexMetaData mdDynamic = newMeta(getIndexMetaData("test_dynamic", dynamicBuilder), "test_dynamic");
        assertThat(mdDynamic.columnPolicy(), is(ColumnPolicy.DYNAMIC));

        XContentBuilder missingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
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
        DocIndexMetaData mdMissing = newMeta(getIndexMetaData("test_missing", missingBuilder), "test_missing");
        assertThat(mdMissing.columnPolicy(), is(ColumnPolicy.DYNAMIC));

        XContentBuilder wrongBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", "wrong")
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
        DocIndexMetaData mdWrong = newMeta(getIndexMetaData("test_wrong", wrongBuilder), "test_wrong");
        assertThat(mdWrong.columnPolicy(), is(ColumnPolicy.DYNAMIC));
    }

    @Test
    public void testCreateArrayMapping() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  tags array(string)," +
                                                               "  scores array(short)" +
                                                               ")");
        assertThat(md.references().get(ColumnIdent.fromPath("tags")).valueType(),
            is((DataType) new ArrayType(DataTypes.STRING)));
        assertThat(md.references().get(ColumnIdent.fromPath("scores")).valueType(),
            is((DataType) new ArrayType(DataTypes.SHORT)));
    }

    @Test
    public void testCreateObjectArrayMapping() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  tags array(object(strict) as (" +
                                                               "    size double index off," +
                                                               "    numbers array(integer)," +
                                                               "    quote string index using fulltext" +
                                                               "  ))" +
                                                               ")");
        assertThat(md.references().get(ColumnIdent.fromPath("tags")).valueType(),
            is((DataType) new ArrayType(DataTypes.OBJECT)));
        assertThat(md.references().get(ColumnIdent.fromPath("tags")).columnPolicy(),
            is(ColumnPolicy.STRICT));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.size")).valueType(),
            is((DataType) DataTypes.DOUBLE));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.size")).indexType(),
            is(Reference.IndexType.NO));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.numbers")).valueType(),
            is((DataType) new ArrayType(DataTypes.INTEGER)));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.quote")).valueType(),
            is((DataType) DataTypes.STRING));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.quote")).indexType(),
            is(Reference.IndexType.ANALYZED));
    }

    @Test
    public void testNoBackwardCompatibleArrayMapping() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .field("primary_keys", "id")
            .startObject("columns")
            .startObject("array_col")
            .field("collection_type", "array")
            .endObject()
            .startObject("nested")
            .startObject("properties")
            .startObject("inner_nested")
            .field("collection_type", "array")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
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
            .startObject("array_col")
            .field("type", "ip")
            .field("index", "not_analyzed")
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
        IndexMetaData indexMetaData = getIndexMetaData("test1", builder);
        DocIndexMetaData docIndexMetaData = newMeta(indexMetaData, "test1");

        // ARRAY TYPES NOT DETECTED
        assertThat(docIndexMetaData.references().get(ColumnIdent.fromPath("array_col")).valueType(),
            is((DataType) DataTypes.IP));
        assertThat(docIndexMetaData.references().get(ColumnIdent.fromPath("nested.inner_nested")).valueType(),
            is((DataType) DataTypes.TIMESTAMP));
    }

    @Test
    public void testNewArrayMapping() throws Exception {
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
            .startObject("array_col")
            .field("type", "array")
            .startObject("inner")
            .field("type", "ip")
            .field("index", "not_analyzed")
            .endObject()
            .endObject()
            .startObject("nested")
            .field("type", "object")
            .startObject("properties")
            .startObject("inner_nested")
            .field("type", "array")
            .startObject("inner")
            .field("type", "date")
            .field("index", "not_analyzed")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetaData indexMetaData = getIndexMetaData("test1", builder);
        DocIndexMetaData docIndexMetaData = newMeta(indexMetaData, "test1");
        assertThat(docIndexMetaData.references().get(ColumnIdent.fromPath("array_col")).valueType(),
            is((DataType) new ArrayType(DataTypes.IP)));
        assertThat(docIndexMetaData.references().get(ColumnIdent.fromPath("nested.inner_nested")).valueType(),
            is((DataType) new ArrayType(DataTypes.TIMESTAMP)));
    }

    @Test
    public void testMergePartitionWithDifferentShardsAndReplicas() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .field("primary_keys", "id")
            .startArray("partitioned_by")
            .startArray()
            .value("datum").value("date")
            .endArray()
            .endArray()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("index", "not_analyzed")
            .endObject()
            .endObject();
        Settings templateSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 6)
            .build();
        IndexMetaData metaData = getIndexMetaData("test1", builder, templateSettings, null);
        DocIndexMetaData md = newMeta(metaData, "test1");

        PartitionName partitionName = new PartitionName("test1", Arrays.asList(new BytesRef("0")));
        Settings partitionSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 10)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 50)
            .build();
        AliasMetaData aliasMetaData = AliasMetaData.newAliasMetaDataBuilder("test1")
            .build();
        IndexMetaData partitionMetaData = getIndexMetaData(partitionName.asIndexName(), builder,
            partitionSettings, aliasMetaData);
        DocIndexMetaData partitionMD = newMeta(partitionMetaData, partitionName.asIndexName());
        assertThat(partitionMD.aliases().size(), is(1));
        assertThat(partitionMD.aliases(), hasItems("test1"));
        assertThat(partitionMD.isAlias(), is(false));
        assertThat(partitionMD.concreteIndexName(), is(partitionName.asIndexName()));
        DocIndexMetaData merged = md.merge(partitionMD, mock(TransportPutIndexTemplateAction.class), true);

        assertThat(merged.numberOfReplicas(), is(BytesRefs.toBytesRef(2)));
        assertThat(merged.numberOfShards(), is(6));
    }

    @Test
    public void testStringArrayWithFulltextIndex() throws Exception {
        DocIndexMetaData metaData = getDocIndexMetaDataFromStatement(
            "create table t (tags array(string) index using fulltext)");

        Reference reference = metaData.columns().get(0);
        assertThat(reference.valueType(), equalTo((DataType) new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCreateTableWithNestedPrimaryKey() throws Exception {
        DocIndexMetaData metaData = getDocIndexMetaDataFromStatement("create table t (o object as (x int primary key))");
        assertThat(metaData.primaryKey(), contains(new ColumnIdent("o", "x")));

        metaData = getDocIndexMetaDataFromStatement("create table t (x object as (y object as (z int primary key)))");
        assertThat(metaData.primaryKey(), contains(new ColumnIdent("x", Arrays.asList("y", "z"))));
    }

    @Test
    public void testSchemaEquals() throws Exception {
        DocIndexMetaData md = getDocIndexMetaDataFromStatement("create table schema_equals1 (id byte, tags array(string))");
        DocIndexMetaData mdSame = getDocIndexMetaDataFromStatement("create table schema_equals1 (id byte, tags array(string))");
        DocIndexMetaData mdOther = getDocIndexMetaDataFromStatement("create table schema_equals2 (id byte, tags array(string))");
        DocIndexMetaData mdWithPk = getDocIndexMetaDataFromStatement("create table schema_equals3 (id byte primary key, tags array(string))");
        DocIndexMetaData mdWithStringCol = getDocIndexMetaDataFromStatement("create table schema_equals4 (id byte, tags array(string), col string)");
        DocIndexMetaData mdWithStringColNotAnalyzed = getDocIndexMetaDataFromStatement("create table schema_equals5 (id byte, tags array(string), col string index off)");
        DocIndexMetaData mdWithStringColNotAnalyzedAndIndex = getDocIndexMetaDataFromStatement("create table schema_equals6 (id byte, tags array(string), col string index off, index ft_index using fulltext(col))");
        assertThat(md.schemaEquals(md), is(true));
        assertThat(md == mdSame, is(false));
        assertThat(md.schemaEquals(mdSame), is(true));   // same table name
        assertThat(md.schemaEquals(mdOther), is(false)); // different table name
        assertThat(md.schemaEquals(mdWithPk), is(false));
        assertThat(md.schemaEquals(mdWithStringCol), is(false));
        assertThat(mdWithPk.schemaEquals(mdWithStringCol), is(false));
        assertThat(mdWithStringCol.schemaEquals(mdWithStringColNotAnalyzed), is(false));
        assertThat(mdWithStringColNotAnalyzed.schemaEquals(mdWithStringColNotAnalyzedAndIndex), is(false));
    }

    @Test
    public void testSchemaWithGeneratedColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .startObject("generated_columns")
            .field("week", "date_trunc('week', ts)")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("ts").field("type", "date").endObject()
            .startObject("week").field("type", "long").endObject()
            .endObject();

        IndexMetaData metaData = getIndexMetaData("test1", builder, Settings.EMPTY, null);
        DocIndexMetaData md = newMeta(metaData, "test1");

        assertThat(md.columns().size(), is(2));
        Reference week = md.references().get(new ColumnIdent("week"));
        assertThat(week, Matchers.notNullValue());
        assertThat(week, instanceOf(GeneratedReference.class));
        assertThat(((GeneratedReference) week).formattedGeneratedExpression(), is("date_trunc('week', ts)"));
        assertThat(((GeneratedReference) week).generatedExpression(), isFunction("date_trunc", isLiteral("week"), isReference("ts")));
        assertThat(((GeneratedReference) week).referencedReferences(), contains(isReference("ts")));
    }

    @Test
    public void testCopyToWithoutMetaIndices() throws Exception {
        // regression test... this mapping used to cause an NPE
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("description")
            .field("type", "string")
            .field("index", "not_analyzed")
            .array("copy_to", "description_ft")
            .endObject()
            .startObject("description_ft")
            .field("type", "string")
            .field("analyzer", "english")
            .endObject()
            .endObject()
            .endObject();

        IndexMetaData metaData = getIndexMetaData("test1", builder, Settings.EMPTY, null);
        DocIndexMetaData md = newMeta(metaData, "test1");

        assertThat(md.indices().size(), is(1));
        assertThat(md.indices().keySet().iterator().next(), is(new ColumnIdent("description_ft")));
    }
}



