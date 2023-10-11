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

package org.elasticsearch.index.mapper;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.mapper.MapperRegistry;

import io.crate.Constants;
import io.crate.server.xcontent.LoggingDeprecationHandler;

public class MapperService extends AbstractIndexComponent implements Closeable {

    /**
     * The reason why a mapping is being merged.
     */
    public enum MergeReason {
        /**
         * Create or update a mapping.
         */
        MAPPING_UPDATE,
        /**
         * Recovery of an existing mapping, for instance because of a restart,
         * if a shard was moved to a different node or for administrative
         * purposes.
         */
        MAPPING_RECOVERY;
    }

    public static final Setting<Long> INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.total_fields.limit", 1000L, 0, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_DEPTH_LIMIT_SETTING =
            Setting.longSetting("index.mapping.depth.limit", 20L, 1, Property.Dynamic, Property.IndexScope);

    private final IndexAnalyzers indexAnalyzers;

    private volatile DocumentMapper mapper;
    private volatile FieldTypeLookup fieldTypes;
    private volatile Map<String, ObjectMapper> fullPathObjectMappers = emptyMap();

    private final DocumentMapperParser documentParser;

    private final MapperAnalyzerWrapper indexAnalyzer;
    private final MapperAnalyzerWrapper searchAnalyzer;
    private final MapperAnalyzerWrapper searchQuoteAnalyzer;

    final MapperRegistry mapperRegistry;

    public MapperService(IndexSettings indexSettings,
                         IndexAnalyzers indexAnalyzers,
                         NamedXContentRegistry xContentRegistry,
                         MapperRegistry mapperRegistry) {
        super(indexSettings);
        this.indexAnalyzers = indexAnalyzers;
        this.fieldTypes = new FieldTypeLookup();
        this.documentParser = new DocumentMapperParser(
            this,
            xContentRegistry,
            mapperRegistry
        );
        this.indexAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultIndexAnalyzer(), MappedFieldType::indexAnalyzer);
        this.searchAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchAnalyzer(), MappedFieldType::searchAnalyzer);
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchQuoteAnalyzer(), MappedFieldType::searchQuoteAnalyzer);
        this.mapperRegistry = mapperRegistry;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public DocumentMapperParser documentMapperParser() {
        return this.documentParser;
    }

    public FieldType getLuceneFieldType(String field) {
        Mapper mapper = documentMapper().mappers().getMapper(field);
        if (mapper == null) {
            return null;
        }
        if (mapper instanceof FieldMapper == false) {
            return null;
        }
        return ((FieldMapper) mapper).fieldType;
    }

    /**
     * Parses the mappings (formatted as JSON) into a map
     */
    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, String mappingSource) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)) {
            return parser.map();
        }
    }

    /**
     * Update mapping by only merging the metadata that is different between received and stored entries
     */
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index()) : "index mismatch: expected " + index()
            + " but was " + newIndexMetadata.getIndex();

        if (currentIndexMetadata != null && currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
            assertMappingVersion(currentIndexMetadata, newIndexMetadata, mapper);
            return false;
        }

        final DocumentMapper updatedMapper;
        try {
            // only update entries if needed
            updatedMapper = internalMerge(newIndexMetadata, MergeReason.MAPPING_RECOVERY);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        if (updatedMapper == null) {
            return false;
        }

        boolean requireRefresh = false;

        assertMappingVersion(currentIndexMetadata, newIndexMetadata, updatedMapper);

        MappingMetadata mappingMetadata = newIndexMetadata.mapping();
        CompressedXContent incomingMappingSource = mappingMetadata.source();

        String op = mapper != null ? "updated" : "added";
        if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
            logger.debug("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
        } else if (logger.isTraceEnabled()) {
            logger.trace("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
        } else {
            logger.debug("[{}] {} mapping (source suppressed due to length, use TRACE level if needed)",
                index(), op);
        }

        // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
        // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
        // merge version of it, which it does when refreshing the mappings), and warn log it.
        if (documentMapper().mappingSource().equals(incomingMappingSource) == false) {
            logger.debug("[{}] parsed mapping, and got different sources\noriginal:\n{}\nparsed:\n{}",
                index(), incomingMappingSource, documentMapper().mappingSource());

            requireRefresh = true;
        }


        return requireRefresh;
    }

    private void assertMappingVersion(
            final IndexMetadata currentIndexMetadata,
            final IndexMetadata newIndexMetadata,
            final DocumentMapper updatedMapper) throws IOException {
        if (Assertions.ENABLED && currentIndexMetadata != null) {
            if (currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedMapper == mapper;

                MappingMetadata mapping = newIndexMetadata.mapping();
                if (mapping != null) {
                    final CompressedXContent currentSource = currentIndexMetadata.mapping().source();
                    final CompressedXContent newSource = mapping.source();
                    assert currentSource.equals(newSource) :
                        "expected current mapping [" + currentSource + "] for type [" + Constants.DEFAULT_MAPPING_TYPE + "] "
                            + "to be the same as new mapping [" + newSource + "]";
                    final CompressedXContent mapperSource = new CompressedXContent(Strings.toString(mapper));
                    assert currentSource.equals(mapperSource) :
                        "expected current mapping [" + currentSource + "] for type [" + Constants.DEFAULT_MAPPING_TYPE + "] "
                            + "to be the same as new mapping [" + mapperSource + "]";
                }
            } else {
                // the mapping version should increase, there should be updates, and the mapping should be different
                final long currentMappingVersion = currentIndexMetadata.getMappingVersion();
                final long newMappingVersion = newIndexMetadata.getMappingVersion();
                assert currentMappingVersion < newMappingVersion :
                        "expected current mapping version [" + currentMappingVersion + "] "
                                + "to be less than new mapping version [" + newMappingVersion + "]";
                assert updatedMapper != null;
                final MappingMetadata currentMapping = currentIndexMetadata.mapping();
                if (currentMapping != null) {
                    final CompressedXContent currentSource = currentMapping.source();
                    final CompressedXContent newSource = updatedMapper.mappingSource();
                    assert currentSource.equals(newSource) == false :
                        "expected current mapping [" + currentSource + "] to be different than new mapping";
                }
            }
        }
    }

    public DocumentMapper merge(Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(Strings.toString(JsonXContent.builder().map(mappings)));
        return internalMerge(content, reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        internalMerge(indexMetadata, reason);
    }

    public DocumentMapper merge(CompressedXContent mappingSource, MergeReason reason) {
        return internalMerge(mappingSource, reason);
    }

    private synchronized DocumentMapper internalMerge(IndexMetadata indexMetadata,
                                                      MergeReason reason) {
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            return internalMerge(mappingMetadata.source(), reason);
        }
        return null;
    }

    private synchronized DocumentMapper internalMerge(CompressedXContent mappings, MergeReason reason) {

        DocumentMapper documentMapper;

        try {
            documentMapper = documentParser.parse(mappings);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }
        return internalMerge(documentMapper, reason);
    }

    private synchronized DocumentMapper internalMerge(DocumentMapper mapper, MergeReason reason) {
        Map<String, ObjectMapper> fullPathObjectMappers = this.fullPathObjectMappers;

        assert mapper != null;

        // compute the merged DocumentMapper
        DocumentMapper oldMapper = this.mapper;
        DocumentMapper newMapper;
        if (oldMapper != null) {
            newMapper = oldMapper.merge(mapper.mapping());
        } else {
            newMapper = mapper;
        }

        // check basic sanity of the new mapping
        List<ObjectMapper> objectMappers = new ArrayList<>();
        List<FieldMapper> fieldMappers = new ArrayList<>();
        MetadataFieldMapper[] metadataMappers = newMapper.mapping().metadataMappers;
        Collections.addAll(fieldMappers, metadataMappers);
        MapperUtils.collect(newMapper.mapping().root(), objectMappers, fieldMappers);

        MapperMergeValidator.validateNewMappers(objectMappers, fieldMappers);

        this.fieldTypes = new FieldTypeLookup(fieldMappers);

        for (ObjectMapper objectMapper : objectMappers) {
            if (fullPathObjectMappers == this.fullPathObjectMappers) {
                // first time through the loops
                fullPathObjectMappers = new HashMap<>(this.fullPathObjectMappers);
            }
            var path = objectMapper.columnOID() == COLUMN_OID_UNASSIGNED ? objectMapper.fullPath() : Long.toString(objectMapper.columnOID());
            fullPathObjectMappers.put(path, objectMapper);
        }

        if (reason == MergeReason.MAPPING_UPDATE) {
            // this check will only be performed on the master node when there is
            // a call to the update mapping API. For all other cases like
            // the master node restoring mappings from disk or data nodes
            // deserializing cluster state that was sent by the master node,
            // this check will be skipped.
            checkTotalFieldsLimit(
                objectMappers.size() + fieldMappers.size() - metadataMappers.length);
            checkDepthLimit(fullPathObjectMappers.keySet());
        }

        // only need to immutably rewrap these if the previous reference was changed.
        // if not then they are already implicitly immutable.
        if (fullPathObjectMappers != this.fullPathObjectMappers) {
            fullPathObjectMappers = Collections.unmodifiableMap(fullPathObjectMappers);
        }

        this.mapper = newMapper;
        this.fullPathObjectMappers = fullPathObjectMappers;

        assert assertSerialization(newMapper);
        return newMapper;
    }

    private boolean assertSerialization(DocumentMapper mapper) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        DocumentMapper newMapper = parse(mappingSource);

        if (newMapper.mappingSource().equals(mappingSource) == false) {
            throw new IllegalStateException("DocumentMapper serialization result is different from source. \n--> Source ["
                + mappingSource + "]\n--> Result ["
                + newMapper.mappingSource() + "]");
        }
        return true;
    }

    private void checkTotalFieldsLimit(long totalMappers) {
        long allowedTotalFields = indexSettings.getValue(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING);
        if (allowedTotalFields < totalMappers) {
            throw new IllegalArgumentException("Limit of total fields [" + allowedTotalFields + "] in index [" + index().getName() + "] has been exceeded");
        }
    }

    private void checkDepthLimit(Collection<String> objectPaths) {
        final long maxDepth = indexSettings.getValue(INDEX_MAPPING_DEPTH_LIMIT_SETTING);
        for (String objectPath : objectPaths) {
            checkDepthLimit(objectPath, maxDepth);
        }
    }

    private void checkDepthLimit(String objectPath, long maxDepth) {
        int numDots = 0;
        for (int i = 0; i < objectPath.length(); ++i) {
            if (objectPath.charAt(i) == '.') {
                numDots += 1;
            }
        }
        final int depth = numDots + 2;
        if (depth > maxDepth) {
            throw new IllegalArgumentException("Limit of mapping depth [" + maxDepth + "] in index [" + index().getName()
                + "] has been exceeded due to object field [" + objectPath + "]");
        }
    }

    public DocumentMapper parse(CompressedXContent mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingSource);
    }

    /**
     * Return the {@link DocumentMapper} for the given type. By using the special
     * {@link Constants#DEFAULT_MAPPING_TYPE} type, you can get a {@link DocumentMapper} for
     * the default mapping.
     */
    public DocumentMapper documentMapper() {
        return mapper;
    }

    /**
     * Returns the {@link MappedFieldType} for the give fullName.
     *
     * If multiple types have fields with the same full name, the first is returned.
     */
    public MappedFieldType fieldType(String fullName) {
        return fieldTypes.get(fullName);
    }

    public ObjectMapper getObjectMapper(String name) {
        return fullPathObjectMappers.get(name);
    }

    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuoteAnalyzer;
    }

    @Override
    public void close() throws IOException {
        indexAnalyzers.close();
    }

    /** An analyzer wrapper that can lookup fields within the index mappings */
    final class MapperAnalyzerWrapper extends DelegatingAnalyzerWrapper {

        private final Analyzer defaultAnalyzer;
        private final Function<MappedFieldType, Analyzer> extractAnalyzer;

        MapperAnalyzerWrapper(Analyzer defaultAnalyzer, Function<MappedFieldType, Analyzer> extractAnalyzer) {
            super(Analyzer.PER_FIELD_REUSE_STRATEGY);
            this.defaultAnalyzer = defaultAnalyzer;
            this.extractAnalyzer = extractAnalyzer;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            MappedFieldType fieldType = fieldType(fieldName);
            if (fieldType != null) {
                Analyzer analyzer = extractAnalyzer.apply(fieldType);
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return defaultAnalyzer;
        }
    }

}
