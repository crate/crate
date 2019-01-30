/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SeedUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public abstract class AbstractBuilderTestCase extends ESTestCase {

    public static final String STRING_FIELD_NAME = "mapped_string";
    public static final String STRING_ALIAS_FIELD_NAME = "mapped_string_alias";
    protected static final String STRING_FIELD_NAME_2 = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String INT_ALIAS_FIELD_NAME = "mapped_int_field_alias";
    protected static final String INT_RANGE_FIELD_NAME = "mapped_int_range";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String DATE_ALIAS_FIELD_NAME = "mapped_date_alias";
    protected static final String DATE_RANGE_FIELD_NAME = "mapped_date_range";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_POINT_ALIAS_FIELD_NAME = "mapped_geo_point_alias";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[]{STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME,
        INT_FIELD_NAME, INT_RANGE_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME,
        DATE_RANGE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME,
        GEO_SHAPE_FIELD_NAME};
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[]{STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME,
        INT_FIELD_NAME, INT_RANGE_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME,
        DATE_FIELD_NAME, DATE_RANGE_FIELD_NAME,  GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME};

    private static final Map<String, String> ALIAS_TO_CONCRETE_FIELD_NAME = new HashMap<>();
    static {
        ALIAS_TO_CONCRETE_FIELD_NAME.put(STRING_ALIAS_FIELD_NAME, STRING_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(INT_ALIAS_FIELD_NAME, INT_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(DATE_ALIAS_FIELD_NAME, DATE_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(GEO_POINT_ALIAS_FIELD_NAME, GEO_POINT_FIELD_NAME);
    }

    private static ServiceHolder serviceHolder;
    private static int queryNameId = 0;
    private static Settings nodeSettings;
    private static Index index;
    private static String[] currentTypes;
    protected static String[] randomTypes;
    private static long nowInMillis;

    protected static Index getIndex() {
        return index;
    }

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    protected static boolean isSingleType() {
        return serviceHolder.idxSettings.isSingleType();
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
    }

    @BeforeClass
    public static void beforeClass() {
        nodeSettings = Settings.builder()
                .put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        index = new Index(randomAlphaOfLengthBetween(1, 10), "_na_");
        nowInMillis = randomNonNegativeLong();

        // Set a single type in the index
        switch (random().nextInt(3)) {
        case 0:
            currentTypes = new String[0]; // no types
            break;
        default:
            currentTypes = new String[] { "_doc" };
            break;
        }
        randomTypes = getRandomTypes();
    }

    private static String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[]{MetaData.ALL};
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return serviceHolder.xContentRegistry;
    }

    protected NamedWriteableRegistry namedWriteableRegistry() {
        return serviceHolder.namedWriteableRegistry;
    }

    /**
     * make sure query names are unique by suffixing them with increasing counter
     */
    protected static String createUniqueRandomName() {
        String queryName = randomAlphaOfLengthBetween(1, 10) + queryNameId;
        queryNameId++;
        return queryName;
    }

    protected Settings createTestIndexSettings() {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version indexVersionCreated = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        return Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersionCreated)
            .build();
    }

    protected static IndexSettings indexSettings() {
        return serviceHolder.idxSettings;
    }

    protected static String expectedFieldName(String builderFieldName) {
        if (currentTypes.length == 0 || !isSingleType()) {
            return builderFieldName;
        }
        return ALIAS_TO_CONCRETE_FIELD_NAME.getOrDefault(builderFieldName, builderFieldName);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        IOUtils.close(serviceHolder);
        serviceHolder = null;
    }

    @Before
    public void beforeTest() throws Exception {
        if (serviceHolder == null) {
            // we initialize the serviceHolder and serviceHolderWithNoType just once, but need some
            // calls to the randomness source during its setup. In order to not mix these calls with
            // the randomness source that is later used in the test method, we use the master seed during
            // this setup
            long masterSeed = SeedUtils.parseSeed(RandomizedTest.getContext().getRunnerSeedAsString());
            RandomizedTest.getContext().runWithPrivateRandomness(masterSeed, (Callable<Void>) () -> {
                serviceHolder = new ServiceHolder(nodeSettings, createTestIndexSettings(), getPlugins(), nowInMillis,
                        AbstractBuilderTestCase.this);
                return null;
            });
        }

        serviceHolder.clientInvocationHandler.delegate = this;
    }

    protected static SearchContext getSearchContext(String[] types, QueryShardContext context) {
        TestSearchContext testSearchContext = new TestSearchContext(context) {
            @Override
            public MapperService mapperService() {
                return serviceHolder.mapperService; // need to build / parse inner hits sort fields
            }

            @Override
            public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
                return serviceHolder.indexFieldDataService.getForField(fieldType); // need to build / parse inner hits sort fields
            }

        };
        testSearchContext.getQueryShardContext().setTypes(types);
        return testSearchContext;
    }

    @After
    public void afterTest() {
        serviceHolder.clientInvocationHandler.delegate = null;
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
    }

    /**
     * @return a new {@link QueryShardContext} with the provided reader
     */
    protected static QueryShardContext createShardContext(IndexReader reader) {
        return serviceHolder.createShardContext(reader);
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        return createShardContext(null);
    }

    private static class ClientInvocationHandler implements InvocationHandler {
        AbstractBuilderTestCase delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(Client.class.getMethod("get", GetRequest.class, ActionListener.class))){
                GetResponse getResponse = delegate.executeGet((GetRequest) args[0]);
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
                if (randomBoolean()) {
                    listener.onResponse(getResponse);
                } else {
                    new Thread(() -> listener.onResponse(getResponse)).start();
                }
                return null;
            } else if (method.equals(Client.class.getMethod
                ("multiTermVectors", MultiTermVectorsRequest.class))) {
                return new PlainActionFuture<MultiTermVectorsResponse>() {
                    @Override
                    public MultiTermVectorsResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeMultiTermVectors((MultiTermVectorsRequest) args[0]);
                    }
                };
            } else if (method.equals(Object.class.getMethod("toString"))) {
                return "MockClient";
            }
            throw new UnsupportedOperationException("this test can't handle calls to: " + method);
        }

    }

    private static class ServiceHolder implements Closeable {
        private final IndexFieldDataService indexFieldDataService;
        private final SearchModule searchModule;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final NamedXContentRegistry xContentRegistry;
        private final ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();
        private final IndexSettings idxSettings;
        private final SimilarityService similarityService;
        private final MapperService mapperService;
        private final BitsetFilterCache bitsetFilterCache;
        private final ScriptService scriptService;
        private final Client client;
        private final long nowInMillis;

        ServiceHolder(Settings nodeSettings, Settings indexSettings, Collection<Class<? extends Plugin>> plugins, long nowInMillis,
                AbstractBuilderTestCase testCase) throws IOException {
            Environment env = InternalSettingsPreparer.prepareEnvironment(nodeSettings, null);
            this.nowInMillis = nowInMillis;
            PluginsService pluginsService;
            pluginsService = new PluginsService(nodeSettings, null, env.modulesFile(), env.pluginsFile(), plugins);

            client = (Client) Proxy.newProxyInstance(
                    Client.class.getClassLoader(),
                    new Class[]{Client.class},
                    clientInvocationHandler);
            ScriptModule scriptModule = createScriptModule(pluginsService.filterPlugins(ScriptPlugin.class));
            List<Setting<?>> additionalSettings = pluginsService.getPluginSettings();
            SettingsModule settingsModule =
                    new SettingsModule(nodeSettings, additionalSettings, pluginsService.getPluginSettingsFilter(), Collections.emptySet());
            searchModule = new SearchModule(nodeSettings, false, pluginsService.filterPlugins(SearchPlugin.class));
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(indicesModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            namedWriteableRegistry = new NamedWriteableRegistry(entries);
            xContentRegistry = new NamedXContentRegistry(Stream.of(
                    searchModule.getNamedXContents().stream()
                    ).flatMap(Function.identity()).collect(toList()));
            IndexScopedSettings indexScopedSettings = settingsModule.getIndexScopedSettings();
            idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings, indexScopedSettings);
            AnalysisModule analysisModule = new AnalysisModule(TestEnvironment.newEnvironment(nodeSettings), emptyList());
            IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(idxSettings);
            scriptService = scriptModule.getScriptService();
            similarityService = new SimilarityService(idxSettings, null, Collections.emptyMap());
            MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
            mapperService = new MapperService(idxSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                    () -> createShardContext(null));
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(nodeSettings, new IndexFieldDataCache.Listener() {
            });
            indexFieldDataService = new IndexFieldDataService(idxSettings, indicesFieldDataCache,
                    new NoneCircuitBreakerService(), mapperService);
            bitsetFilterCache = new BitsetFilterCache(idxSettings, new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {

                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {

                }
            });


            for (String type : currentTypes) {
                mapperService.merge(type, new CompressedXContent(Strings.toString(PutMappingRequest.buildFromSimplifiedDef(type,
                    STRING_FIELD_NAME, "type=text",
                    STRING_FIELD_NAME_2, "type=keyword",
                    INT_FIELD_NAME, "type=integer",
                    INT_RANGE_FIELD_NAME, "type=integer_range",
                    DOUBLE_FIELD_NAME, "type=double",
                    BOOLEAN_FIELD_NAME, "type=boolean",
                    DATE_FIELD_NAME, "type=date",
                    DATE_RANGE_FIELD_NAME, "type=date_range",
                    OBJECT_FIELD_NAME, "type=object",
                    GEO_POINT_FIELD_NAME, "type=geo_point",
                    GEO_SHAPE_FIELD_NAME, "type=geo_shape"
                ))), MapperService.MergeReason.MAPPING_UPDATE, false);

                // Field aliases are only supported on indexes with a single type. If the index has multiple types, we
                // still create fields with the same names as the alias fields, but with a concrete definition. This
                // avoids the need for various test classes to check whether the index contains a single type.
                if (idxSettings.isSingleType()) {
                    mapperService.merge(type, new CompressedXContent(Strings.toString(PutMappingRequest.buildFromSimplifiedDef(type,
                        STRING_ALIAS_FIELD_NAME, "type=alias,path=" + STRING_FIELD_NAME,
                        INT_ALIAS_FIELD_NAME, "type=alias,path=" + INT_FIELD_NAME,
                        DATE_ALIAS_FIELD_NAME, "type=alias,path=" + DATE_FIELD_NAME,
                        GEO_POINT_ALIAS_FIELD_NAME, "type=alias,path=" + GEO_POINT_FIELD_NAME
                    ))), MapperService.MergeReason.MAPPING_UPDATE, false);
                } else {
                    mapperService.merge(type, new CompressedXContent(Strings.toString(PutMappingRequest.buildFromSimplifiedDef(type,
                        STRING_ALIAS_FIELD_NAME, "type=text",
                        INT_ALIAS_FIELD_NAME, "type=integer",
                        DATE_ALIAS_FIELD_NAME, "type=date",
                        GEO_POINT_ALIAS_FIELD_NAME, "type=geo_point"
                    ))), MapperService.MergeReason.MAPPING_UPDATE, false);
                }

                // also add mappings for two inner field in the object field
                mapperService.merge(type, new CompressedXContent("{\"properties\":{\"" + OBJECT_FIELD_NAME + "\":{\"type\":\"object\","
                                + "\"properties\":{\"" + DATE_FIELD_NAME + "\":{\"type\":\"date\"},\"" +
                                INT_FIELD_NAME + "\":{\"type\":\"integer\"}}}}}"),
                        MapperService.MergeReason.MAPPING_UPDATE, false);
            }
            testCase.initializeAdditionalMappings(mapperService);
        }

        @Override
        public void close() throws IOException {
        }

        QueryShardContext createShardContext(IndexReader reader) {
            return new QueryShardContext(0, idxSettings, bitsetFilterCache, indexFieldDataService::getForField, mapperService,
                similarityService, scriptService, xContentRegistry, namedWriteableRegistry, this.client, reader, () -> nowInMillis, null);
        }

        ScriptModule createScriptModule(List<ScriptPlugin> scriptPlugins) {
            if (scriptPlugins == null || scriptPlugins.isEmpty()) {
                return newTestScriptModule();
            }
            return new ScriptModule(Settings.EMPTY, scriptPlugins);
        }
    }

}
