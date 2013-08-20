package crate.elasticsearch.import_;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
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
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;

import crate.elasticsearch.action.import_.ImportContext;
import crate.elasticsearch.action.import_.NodeImportRequest;

public class Importer {

    private Client client;
    private final Injector injector;

    private final ByteSizeValue bulkByteSize = new ByteSizeValue(5, ByteSizeUnit.MB);
    private final TimeValue flushInterval = TimeValue.timeValueSeconds(5);
    private final int concurrentRequests = 4;

    @Inject
    public Importer(Injector injector) {
        this.injector = injector;
    }

    public Result execute(ImportContext context, NodeImportRequest request) {
        if (this.client == null) {
            // Inject here to avoid injection loop in constructor
            this.client = injector.getInstance(Client.class);
        }
        String index = request.index();
        String type = request.type();
        int bulkSize = request.bulkSize();
        Result result = new Result();
        Date start = new Date();
        File dir = new File(context.directory());
        if (dir.isDirectory()) {
            File[] files;
            if (context.file_pattern() == null) {
                files = dir.listFiles();
            } else {
                final Pattern file_pattern = context.file_pattern();
                files = dir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        Matcher m = file_pattern.matcher(name);
                        if (m.find()) {
                            return true;
                        }
                        return false;
                    }
                });
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
                throw new ElasticSearchException("::" ,e);
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
        }
        result.took = new Date().getTime() - start.getTime();
        return result;
    }

    private ImportCounts handleFile(File file, String index, String type, int bulkSize, boolean compression) {
        if (file.isFile() && file.canRead()) {
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
                        indexRequest = parseObject(line);
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
            } finally {
                bulkProcessor.close();
            }
            try {
                bulkListener.get();
            } catch (InterruptedException e1) {
            } catch (ExecutionException e1) {
            }
            return bulkListener.importCounts();
        }
        return null;
    }

    private IndexRequest parseObject(String line) throws ObjectImportException {
        XContentParser parser = null;
        try {
            IndexRequest indexRequest = new IndexRequest();
            parser = XContentFactory.xContent(line.getBytes()).createParser(line.getBytes());
            Token token;
            XContentBuilder sourceBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            long ttl = 0;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    token = parser.nextToken();
                    if (fieldName.equals(IdFieldMapper.NAME) && token == Token.VALUE_STRING) {
                        indexRequest.id(parser.text());
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
                    } else if (fieldName.equals(SourceFieldMapper.NAME) && token == Token.START_OBJECT) {
                        sourceBuilder.copyCurrentStructure(parser);
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
            indexRequest.source(sourceBuilder);
            return indexRequest;
        } catch (ElasticSearchParseException e) {
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
            ImmutableMap<String, IndexMetaData> foundIndices = getIndexMetaData(indexes);
            indexes.removeAll(foundIndices.keySet());
        } catch (IndexMissingException e) {
            // all indexes are missing
        }
        return indexes;
    }

    private ImmutableMap<String, IndexMetaData> getIndexMetaData(Set<String> indexes) {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .filterRoutingTable(true)
                .filterNodes(true)
                .filteredIndices(indexes.toArray(new String[indexes.size()]));
        clusterStateRequest.listenerThreaded(false);
        ClusterStateResponse response = client.admin().cluster().state(clusterStateRequest).actionGet();
        return ImmutableMap.copyOf(response.getState().metaData().indices());
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
        Map<String, Object> map = parser.map();
        return map;
    }

    class MappingImportException extends ElasticSearchException {

        private static final long serialVersionUID = 683146198427799700L;

        public MappingImportException(String msg) {
            super(msg);
        }

        public MappingImportException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    class SettingsImportException extends ElasticSearchException {

        private static final long serialVersionUID = -3697101419212831353L;

        public SettingsImportException(String msg) {
            super(msg);
        }

        public SettingsImportException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    class ObjectImportException extends ElasticSearchException {

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
        public int successes = 0;
        public int failures = 0;
        public int invalid = 0;
    }

}
