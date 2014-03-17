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

package io.crate.import_;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import io.crate.action.import_.ImportContext;
import io.crate.action.import_.NodeImportRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.GZIPInputStream;

public class Importer {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private Client client;
    private final Injector injector;
    private final ClusterService clusterService;

    private final ByteSizeValue bulkByteSize = new ByteSizeValue(5, ByteSizeUnit.MB);
    private final TimeValue flushInterval = TimeValue.timeValueSeconds(5);
    private final int concurrentRequests = 4;

    @Inject
    public Importer(Injector injector, ClusterService clusterService) {
        this.injector = injector;
        this.clusterService = clusterService;
    }

    public Result execute(ImportContext context, NodeImportRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] executing import request \n{}",
                    clusterService.localNode().id(), request.source().toUtf8());
        }
        if (this.client == null) {
            // Inject here to avoid injection loop in constructor
            this.client = injector.getInstance(Client.class);
        }
        String index = request.index();
        String type = request.type();
        int bulkSize = request.bulkSize();
        Result result = new Result();
        Date start = new Date();

        File path = new File(context.path());
        Pattern file_pattern = context.file_pattern();
        File[] files;

        if (!path.isDirectory() && !path.isFile() && context.file_pattern() == null) {
            // try to parse last path segment as file pattern
            Tuple<String, Pattern> tuple = parsePathPattern(context.path());
            path = new File(tuple.v1());
            file_pattern = tuple.v2();
        }

        if (path.isDirectory()) {
            if (file_pattern == null) {
                files = path.listFiles();
            } else {
                final Pattern file_pattern_inner = file_pattern;
                files = path.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        Matcher m = file_pattern_inner.matcher(name);
                        if (m.find()) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        } else if (path.isFile()) {
            files = new File[1];
            files[0] = path;
        } else {
            logger.trace("[{}] path is neither file nor directory: '{}'",
                    clusterService.localNode().id(),
                    path.getAbsolutePath());
            return result;
        }
        if (files.length == 0) {
            logger.trace("[{}] no files found for '{}'",
                    clusterService.localNode().id(),
                    path.getAbsolutePath());
            return result;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("[{}] importing into index '{}' from '{}'",
                    clusterService.localNode().id(),
                    request.index(),
                    Collections2.transform(Arrays.asList(files), new Function<File, String>() {
                        @Override
                        public String apply(File input) {
                            return input.getAbsolutePath();
                        }
                    }));
        }

        // import settings according to the given data file pattern
        try {
        if (context.settings()) {
            Set<String> createdSettings = new HashSet<String>();
            for (File file : files) {
                String fileName = file.getName();
                if (!fileName.endsWith(".mapping") && !fileName.endsWith(".settings") && file.isFile() && file.canRead()) {
                    loadSettings(file, createdSettings, index);
                }
            }
        }
        // import mappings according to the given data file pattern
        if (context.mappings()) {
            Map<String, Set<String>> createdMappings = new HashMap<String, Set<String>>();
            for (File file : files) {
                String fileName = file.getName();
                if (!fileName.endsWith(".mapping") && !fileName.endsWith(".settings") && file.isFile() && file.canRead()) {
                    loadMappings(file, createdMappings, index, type);
                }
            }
        }
        } catch (Exception e) {
            logger.trace("[{}] error during import of setting and mappings", e,
                    clusterService.localNode().id());
            throw new ElasticsearchException("::" ,e);
        }
        // import data according to the given data file pattern
        for (File file : files) {
            String fileName = file.getName();
            if (!fileName.endsWith(".mapping") && !fileName.endsWith(".settings")) {
                ImportCounts counts = handleFile(file, index, type, bulkSize, context.compression());
                if (counts != null) {
                    result.importCounts.add(counts);
                }
            }
        }
        result.took = new Date().getTime() - start.getTime();
        return result;
    }

    private ImportCounts handleFile(File file, String index, String type, int bulkSize, boolean compression) {
        if (file.isFile() && file.canRead()) {
            String pk = getPrimaryKey(index);
            ImportBulkListener bulkListener = new ImportBulkListener(file.getAbsolutePath());
            BulkProcessor bulkProcessor = BulkProcessor.builder(client, bulkListener)
                    .setBulkActions(bulkSize)
                    .setBulkSize(bulkByteSize)
                    .setFlushInterval(flushInterval)
                    .setConcurrentRequests(concurrentRequests)
                    .build();
            try {
                BufferedReader r;
                if (compression) {
                    GZIPInputStream is = new GZIPInputStream(new FileInputStream(file));
                    r = new BufferedReader(new InputStreamReader(is));
                } else {
                    r = new BufferedReader(new FileReader(file));
                }
                String line;
                while ((line = r.readLine()) != null) {
                    IndexRequest indexRequest;
                    try {
                        indexRequest = parseObject(line, index, type, pk);
                    } catch (ObjectImportException e) {
                        bulkListener.addFailure();
                        continue;
                    }
                    if (indexRequest != null) {
                        indexRequest.opType(OpType.INDEX);
                        if (index != null) {
                            indexRequest.index(index);
                        }
                        if (type != null) {
                            indexRequest.type(type);
                        }
                        if (indexRequest.type() != null && indexRequest.index() != null) {
                            bulkProcessor.add(indexRequest);
                        } else {
                            bulkListener.addFailure();
                        }
                    } else {
                        bulkListener.addInvalid();
                    }
                }
            } catch (FileNotFoundException e) {
                // Ignore not existing files, actually they should exist, as they are filtered before.
            } catch (IOException e) {
                logger.trace("[{}] error during file import of {} into index {}",
                        clusterService.localNode().id(),
                        file.getAbsolutePath(),
                        index);
            } finally {
                bulkProcessor.close();
            }
            try {
                bulkListener.get();
            } catch (InterruptedException|ExecutionException e1) {
                logger.trace("[{}] error waiting for ImportBulkListener", e1, clusterService.localNode().id());
            }
            return bulkListener.importCounts();
        } else {
            logger.trace("[{}] file '{}' not readable", clusterService.localNode().id(), file.getAbsolutePath());
        }

        return null;
    }

    public static IndexRequest parseObject(String line,
            String index,
            String type, String pk) throws ObjectImportException {
        XContentParser parser;
        try {
            IndexRequest indexRequest = new IndexRequest();
            parser = XContentFactory.xContent(line.getBytes()).createParser(line.getBytes());
            Token token;
            XContentBuilder sourceBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            boolean checkPK = (pk != null);
            long ttl = 0;
            int depth = 0;
            while (true) {
                token = parser.nextToken();
                if (token == Token.START_OBJECT){
                    depth ++;
                } else if (token == Token.END_OBJECT){
                    depth --;
                    if (depth==0){
                        break;
                    }
                }
                if ((depth==1) &&  token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    token = parser.nextToken();
                    if (fieldName.equals(IdFieldMapper.NAME) && token == Token.VALUE_STRING) {
                        indexRequest.id(parser.text());
                        checkPK = false;
                    } else if (fieldName.equals(IndexFieldMapper.NAME) && token == Token.VALUE_STRING) {
                        indexRequest.index(parser.text());
                    } else if (fieldName.equals(TypeFieldMapper.NAME) && token == Token.VALUE_STRING) {
                        indexRequest.type(parser.text());
                    } else if (fieldName.equals(RoutingFieldMapper.NAME) && token == Token.VALUE_STRING) {
                        indexRequest.routing(parser.text());
                    } else if (fieldName.equals(TimestampFieldMapper.NAME) && token == Token.VALUE_NUMBER) {
                        indexRequest.timestamp(String.valueOf(parser.longValue()));
                    } else if (fieldName.equals(TTLFieldMapper.NAME) && token == Token.VALUE_NUMBER) {
                        ttl = parser.longValue();
                    } else if (fieldName.equals("_version") && token == Token.VALUE_NUMBER) {
                        indexRequest.version(parser.longValue());
                        indexRequest.versionType(VersionType.EXTERNAL);
                    } else if (token == Token.START_OBJECT){
                        if (fieldName.equals(SourceFieldMapper.NAME)) {
                            sourceBuilder.copyCurrentStructure(parser);
                        } else {
                            depth ++;
                        }
                    } else if(checkPK && pk.equals(fieldName)) {
                        indexRequest.id(parser.text());
                    }
                } else if (token == null) {
                    break;
                }
            }

            if (ttl > 0) {
                String ts = indexRequest.timestamp();
                long start;
                if (ts != null) {
                    start = Long.valueOf(ts);
                } else {
                    start = new Date().getTime();
                }
                ttl = ttl - start;
                if (ttl > 0) {
                    indexRequest.ttl(ttl);
                } else {
                    // object is invalid, do not import
                    return null;
                }
            }
            if (sourceBuilder.bytes().length() == 0) {
                // treat complete line as source
                indexRequest.source(line.getBytes());
            } else {
                indexRequest.source(sourceBuilder);
            }
            return indexRequest;
        } catch (ElasticsearchParseException e) {
            throw new ObjectImportException(e);
        } catch (IOException e) {
            throw new ObjectImportException(e);
        }
    }

    private void loadSettings(File file, Set<String> createdSettings, String restrictedIndex) {
        File settingsFile = new File(file.getAbsolutePath() + ".settings");
        if (settingsFile.exists() && settingsFile.isFile() && settingsFile.canRead()) {
            Map<String, Object> map;
            try {
                map = getMapFromJSONFile(settingsFile);
            } catch (Exception e) {
                throw new SettingsImportException("Error while reading settings file " + settingsFile.getAbsolutePath(), e);
            }
            if (map != null) {
                Set<String> keys = map.keySet();
                keys.removeAll(createdSettings);
                if (keys.size() > 0) {
                    keys = getMissingIndexes(keys);
                }
                for (String key : keys) {
                    if (restrictedIndex == null || restrictedIndex.equals(key)) {
                        try {
                            Object indexMap = map.get(key);
                            if (indexMap instanceof Map) {
                                Builder builder = ImmutableSettings.settingsBuilder();
                                Object settingsMap = ((Map<String, Object>) indexMap).get("settings");
                                if (settingsMap != null && settingsMap instanceof Map) {
                                    XContentBuilder settingsBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                                    builder.loadFromSource(settingsBuilder.map((Map<String, Object>) settingsMap).string());
                                }
                                Settings settings = builder.build();
                                CreateIndexRequest cir = new CreateIndexRequest(key, settings);
                                try {
                                    client.admin().indices().create(cir).actionGet();
                                } catch (IndexAlreadyExistsException e1) {
                                    // ignore, maybe a concurrent shard created the index simultaneously
                                }
                            }
                        } catch (IOException e) {
                            throw new SettingsImportException("Error while creating index " + key + " from settings file " + settingsFile.getAbsolutePath(), e);
                        }
                    }
                }
            }
        } else {
            throw new SettingsImportException("Settings file " + settingsFile.getAbsolutePath() + " could not be found.");
        }
    }

    private void loadMappings(File file, Map<String, Set<String>> createdMappings, String restrictedIndex, String restrictedType) {
        File mappingFile = new File(file.getAbsolutePath() + ".mapping");
        if (mappingFile.exists() && mappingFile.isFile() && mappingFile.canRead()) {
            Map<String, Object> map;
            try {
                map = getMapFromJSONFile(mappingFile);
            } catch (Exception e) {
                throw new MappingImportException("Error while reading mapping file " + mappingFile.getAbsolutePath(), e);
            }
            if (map != null) {
                for (String index : map.keySet()) {
                    if (restrictedIndex == null || restrictedIndex.equals(index)) {
                        Object o = map.get(index);
                        if (o instanceof Map) {
                            Map<String, Object> typesMap = (Map<String, Object>) o;
                            Set<String> created = createdMappings.get(index);
                            if (created == null) {
                                created = new HashSet<String>();
                                createdMappings.put(index, created);
                            }
                            for (String type : typesMap.keySet()) {
                                if ((restrictedType == null || restrictedType.equals(type)) && !created.contains(type)) {
                                    Object m = typesMap.get(type);
                                    if (m instanceof Map) {
                                        Map<String, Object> mapping = new HashMap<String, Object>();
                                        mapping.put(type, m);
                                        PutMappingRequest mappingRequest = new PutMappingRequest(index);
                                        mappingRequest.type(type);
                                        mappingRequest.source(mapping);
                                        try {
                                            if (client.admin().indices().putMapping(mappingRequest).actionGet().isAcknowledged()) {
                                                created.add(type);
                                            }
                                        } catch (IndexMissingException e) {
                                            throw new MappingImportException("Unable to create mapping. Index " + index + " missing.", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            throw new MappingImportException("Mapping file " + mappingFile.getAbsolutePath() + " could not be found.");
        }

    }

    private Set<String> getMissingIndexes(Set<String> indexes) {
        try {
            ImmutableOpenMap<String, IndexMetaData> foundIndices = getIndexMetaData(indexes);
            for (ObjectCursor<String> cursor : foundIndices.keys()) {
                indexes.remove(cursor.value);
            }
        } catch (IndexMissingException e) {
            // all indexes are missing
        }
        return indexes;
    }

    private ImmutableOpenMap<String, IndexMetaData> getIndexMetaData(Set<String> indexes) {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .routingTable(false)
                .nodes(false)
                .metaData(true)
                .indices(indexes.toArray(new String[indexes.size()]));
        clusterStateRequest.listenerThreaded(false);
        ClusterStateResponse response = client.admin().cluster().state(clusterStateRequest).actionGet();
        return ImmutableOpenMap.builder(response.getState().metaData().indices()).build();
    }



    private Map<String, Object> getMapFromJSONFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        byte[] bytes = sb.toString().getBytes();
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        return parser.map();
    }

    class MappingImportException extends ElasticsearchException {

        private static final long serialVersionUID = 683146198427799700L;

        public MappingImportException(String msg) {
            super(msg);
        }

        public MappingImportException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    class SettingsImportException extends ElasticsearchException {

        private static final long serialVersionUID = -3697101419212831353L;

        public SettingsImportException(String msg) {
            super(msg);
        }

        public SettingsImportException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    static class ObjectImportException extends ElasticsearchException {

        private static final long serialVersionUID = 2405764408378929056L;

        public ObjectImportException(Throwable cause) {
            super("Object could not be imported.", cause);
        }
   }

    public static class Result {
        public List<ImportCounts> importCounts = new ArrayList<Importer.ImportCounts>();
        public long took;
    }

    public static class ImportCounts {
        public String fileName;
        public AtomicLong successes = new AtomicLong(0);
        public AtomicLong failures = new AtomicLong(0);
        public int invalid = 0;
    }

    private Tuple<String, Pattern> parsePathPattern(String path) {
        List<String> pathSplit = Splitter.on(File.separatorChar).splitToList(path);
        Pattern file_pattern = null;
        if (pathSplit.size() >= 2) {
            String basePath = Joiner.on(File.separatorChar).join(pathSplit.subList(0, pathSplit.size() - 1));
            String pattern = pathSplit.get(pathSplit.size()-1);

            try {
                file_pattern = Pattern.compile(pattern);
                path = basePath;
            } catch (PatternSyntaxException e) {
            }
        }
        return new Tuple<>(path, file_pattern);
    }

    private String getPrimaryKey(String index) {
        // TODO: use meta data service from sql once this is package gets integrated
        // This also only supports single column keys

        if (index==null){
            // if a dynamic import is done we do not support primary keys,
            // since this is not from sql in this case
            return null;
        }
        IndexMetaData indexMetaData = clusterService.state().metaData().index(index);
        if (indexMetaData==null){
            return null;
        }
        MappingMetaData mappingMetaData = indexMetaData.mappingOrDefault("default");
        if (mappingMetaData != null) {
            Object metaProperty = null;
            try {
                metaProperty = mappingMetaData.sourceAsMap().get("_meta");
            } catch (IOException e) {
                return null;
            }
            if (metaProperty != null) {
                    Object srcPks = ((Map)metaProperty).get("primary_keys");
                    if (srcPks instanceof String) {
                        return (String)srcPks;
                    } else if (srcPks instanceof List) {
                        if (((List) srcPks).size() == 1){
                            return ((List<String>) srcPks).get(0);
                        }
                    }
            }

        }
        return null;
    }

}
