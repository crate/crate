package org.cratedb.module.import_.test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.cratedb.action.export.ExportAction;
import org.cratedb.action.export.ExportRequest;
import org.cratedb.action.export.ExportResponse;
import org.cratedb.action.import_.ImportAction;
import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.import_.ImportResponse;
import org.cratedb.action.import_.NodeImportResponse;
import org.cratedb.import_.Importer;
import org.cratedb.module.AbstractRestActionTest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.cratedb.test.integration.PathAccessor.stringFromPath;

public class RestImportActionTest extends AbstractRestActionTest {

    /**
     * An import directory must be specified in the post data of the request, otherwise
     * an 'No directory defined' exception is delivered in the output.
     */
    @Test
    public void testNoDirectory() {
        setUpSecondNode();
        ImportResponse response = executeImportRequest("{}");
        assertEquals(0, getImports(response).size());
        List<Map<String, Object>> failures = getImportFailures(response);
        assertEquals(2, failures.size());
        assertTrue(failures.get(0).toString().contains("No path or directory defined"));
    }

    /**
     * A normal import on a single node delivers the node ids of the executing nodes,
     * the time in milliseconds for each node, and the imported files of each nodes
     * with numbers of successful and failing import objects.
     */
    @Test
    public void testImportWithIndexAndType() {
        String path = getClass().getResource("/importdata/import_1").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(1, imports.size());
        Map<String, Object> nodeInfo = imports.get(0);
        assertNotNull(nodeInfo.get("node_id"));
        assertTrue(Long.valueOf(nodeInfo.get("took").toString()) > 0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)/importdata/import_1/import_1.json, successes=2, failures=0\\}\\]"));
        assertTrue(existsWithField("102", "name", "102"));
        assertTrue(existsWithField("103", "name", "103"));
    }

    /**
     * If the type or the index are not given whether in the request URI nor
     * in the import line, the corresponding objects are not imported.
     */
    @Test
    public void testImportWithoutIndexOrType() {
        String path = getClass().getResource("/importdata/import_2").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        Map<String, Object> nodeInfo = imports.get(0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)/importdata/import_2/import_2.json, successes=1, failures=3\\}\\]"));
        assertTrue(existsWithField("202", "name", "202"));
        assertFalse(existsWithField("203", "name", "203"));
        assertFalse(existsWithField("204", "name", "204"));
        assertFalse(existsWithField("205", "name", "205"));
    }

    /**
     * If the index and/or type are given in the URI, all objects are imported
     * into the given index/type.
     */
    @Test
    public void testImportIntoIndexAndType() {
        String path = getClass().getResource("/importdata/import_2").getPath();
        ImportRequest request = new ImportRequest();
        request.index("another_index");
        request.type("e");
        request.source("{\"directory\": \"" + path + "\"}");
        ImportResponse response = client().execute(ImportAction.INSTANCE, request).actionGet();

        List<Map<String, Object>> imports = getImports(response);
        Map<String, Object> nodeInfo = imports.get(0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)/importdata/import_2/import_2.json, successes=4, failures=0\\}\\]"));
        assertTrue(existsWithField("202", "name", "202", "another_index", "e"));
        assertTrue(existsWithField("203", "name", "203", "another_index", "e"));
        assertTrue(existsWithField("204", "name", "204", "another_index", "e"));
        assertTrue(existsWithField("205", "name", "205", "another_index", "e"));
    }

    /**
     * On bad import files, only the readable lines will be imported, the rest is
     * put to the failure count. (e.g. empty lines, or bad JSON structure)
     */
    @Test
    public void testCorruptFile() {
        String path = getClass().getResource("/importdata/import_3").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(1, imports.size());
        assertTrue(imports.get(0).get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)/importdata/import_3/import_3.json, successes=3, failures=2\\}\\]"));
    }

    /**
     * The fields _routing, _ttl and _timestamp can be imported. The ttl value
     * is always from now to the end date, no matter if a time stamp value is set.
     * Invalidated objects will not be imported (when actual time is above ttl time stamp).
     */
    @Test
    public void testFields() {
        prepareCreate("test").setSettings(
            ImmutableSettings.builder().loadFromClasspath("/essetup/settings/test_b.json")
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0).build())
            .addMapping("d", "{\"d\": {\"_timestamp\": {\"enabled\": true, \"store\": \"yes\"}}}")
            .execute()
            .actionGet();
        waitForRelocation(ClusterHealthStatus.GREEN);

        long now = new Date().getTime();
        long ttl = 1867329687097L - now;
        String path = getClass().getResource("/importdata/import_4").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(1, imports.size());
        Map<String, Object> nodeInfo = imports.get(0);
        assertNotNull(nodeInfo.get("node_id"));
        assertTrue(Long.valueOf(nodeInfo.get("took").toString()) > 0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)/importdata/import_4/import_4.json, successes=2, failures=0, invalidated=1}]"));

        GetRequestBuilder rb = new GetRequestBuilder(client(), "test");
        GetResponse res = rb.setType("d").setId("402").setFields("_ttl", "_timestamp", "_routing").execute().actionGet();
        assertEquals("the_routing", res.getField("_routing").getValue());
        assertTrue(ttl - Long.valueOf(res.getField("_ttl").getValue().toString()) < 10000);
        assertEquals(1367329785380L, res.getField("_timestamp").getValue());

        res = rb.setType("d").setId("403").setFields("_ttl", "_timestamp").execute().actionGet();
        assertTrue(ttl - Long.valueOf(res.getField("_ttl").getValue().toString()) < 10000);
        assertTrue(now - Long.valueOf(res.getField("_timestamp").getValue().toString()) < 10000);

        assertFalse(existsWithField("404", "name", "404"));
    }

    /**
     * With multiple nodes every node is handled and delivers correct JSON. Every
     * found file in the given directory on the node's system is handled.
     * Note that this test runs two nodes on the same file system, so the same
     * files are imported twice.
     */
    @Test
    public void testMultipleFilesAndMultipleNodes() {
        setUpSecondNode();
        String path = getClass().getResource("/importdata/import_5").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(2, imports.size());

        String result = "\\[\\{file_name=(.*)/importdata/import_5/import_5_[ab].json, successes=1, failures=0\\}, \\{file_name=(.*)import_5_[ab].json, successes=1, failures=0\\}\\]";
        Map<String, Object> nodeInfo = imports.get(0);
        assertNotNull(nodeInfo.get("node_id"));
        assertTrue(Long.valueOf(nodeInfo.get("took").toString()) > 0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(result));
        nodeInfo = imports.get(1);
        assertNotNull(nodeInfo.get("node_id"));
        assertTrue(Long.valueOf(nodeInfo.get("took").toString()) > 0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(result));

        assertTrue(existsWithField("501", "name", "501"));
        assertTrue(existsWithField("511", "name", "511"));
    }

    /**
     * Some failures may occur in the bulk request results, like Version conflicts.
     * The failures are counted correctly.
     */
    @Test
    public void testFailures() {
        String path = getClass().getResource("/importdata/import_6").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        Map<String, Object> nodeInfo = imports.get(0);
        assertNotNull(nodeInfo.get("node_id"));
        assertTrue(Long.valueOf(nodeInfo.get("took").toString()) > 0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)import_6.json, successes=1, failures=1\\}\\]"));
    }

    /**
     * With the compression flag set to 'gzip' zipped files will be unzipped before
     * importing.
     */
    @Test
    public void testCompression() {
        String path = getClass().getResource("/importdata/import_7").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\", \"compression\":\"gzip\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(1, imports.size());
        Map<String, Object> nodeInfo = imports.get(0);
        assertNotNull(nodeInfo.get("node_id"));
        assertTrue(Long.valueOf(nodeInfo.get("took").toString()) > 0);
        assertTrue(nodeInfo.get("imported_files").toString().matches(
                "\\[\\{file_name=(.*)import_7.json.gz, successes=2, failures=0\\}\\]"));
        assertTrue(existsWithField("102", "name", "102"));
        assertTrue(existsWithField("103", "name", "103"));
    }

    /**
     * Using a relative directory leads to an import from within each node's export
     * directory in the data path. This test also covers the export - import combination.
     */
    @Test
    public void testImportRelativeFilename() throws Exception {
        String node2 = setUpSecondNode();
        // create sample data
        deleteAll();
        prepareCreate("users").setSettings(
            ImmutableSettings.builder().loadFromClasspath("/essetup/settings/test_b.json").build()
        ).addMapping("d", stringFromPath("/essetup/mappings/test_b.json", getClass())).execute().actionGet();

        client().index(new IndexRequest("users", "d", "1").source("{\"name\": \"item1\"}")).actionGet();
        client().index(new IndexRequest("users", "d", "2").source("{\"name\": \"item2\"}")).actionGet();
        refresh();

        makeNodeDataLocationDirectories("myExport");

        // export data and recreate empty index
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source("{\"output_file\": \"myExport/export.${shard}.${index}.json\", \"fields\": [\"_source\", \"_id\", \"_index\", \"_type\"], \"force_overwrite\": true}");
        ExportResponse exportResponse = client().execute(ExportAction.INSTANCE, exportRequest).actionGet();
        assertEquals(0, exportResponse.getFailedShards());

        deleteAll();
        prepareCreate("users").setSettings(
            ImmutableSettings.builder().loadFromClasspath("/essetup/settings/test_b.json").build()
        ).addMapping("d", stringFromPath("/essetup/mappings/test_b.json", getClass())).execute().actionGet();
        waitForRelocation(ClusterHealthStatus.GREEN);

        // run import with relative directory
        ImportResponse response = executeImportRequest("{\"directory\": \"myExport\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(2, imports.size());
        String regex = "\\[\\{file_name=(.*)/nodes/(\\d)/myExport/export.(\\d).users.json, successes=(\\d), failures=0\\}\\]";

        assertTrue(imports.get(0).get("imported_files").toString().matches(regex));
        assertTrue(imports.get(1).get("imported_files").toString().matches(regex));

        assertTrue(existsWithField("1", "name", "item1", "users", "d"));
        assertTrue(existsWithField("2", "name", "item2", "users", "d"));
    }

    /**
     * A file pattern can be specified to filter only for files with a given regex.
     * The other files are not imported.
     */
    @Test
    public void testFilePattern() {
        String path = getClass().getResource("/importdata/import_8").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\", \"file_pattern\": \"index_test_(.*).json\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(1, imports.size());
        Map<String, Object> nodeInfo = imports.get(0);
        List imported = (List) nodeInfo.get("imported_files");
        assertTrue(imported.size() == 1);
        assertTrue(imported.get(0).toString().matches(
                "\\{file_name=(.*)/importdata/import_8/index_test_1.json, successes=2, failures=0\\}"));
        assertTrue(existsWithField("802", "name", "802", "test", "d"));
        assertTrue(existsWithField("803", "name", "803", "test", "d"));
        assertFalse(existsWithField("811", "name", "811", "test", "d"));
        assertFalse(existsWithField("812", "name", "812", "test", "d"));
    }

    /**
     * A bad regex pattern leads to a failure response.
     */
    @Test
    public void testBadFilePattern() {
        String path = getClass().getResource("/importdata/import_8").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\", \"file_pattern\": \"(.*(d|||\"}");
        List<Map<String, Object>> failures = getImportFailures(response);
        assertEquals(1, failures.size());
        assertTrue(failures.toString().contains("PatternSyntaxException: Unclosed group near index"));
    }

    @Test
    public void testSettings() {
        String path = getClass().getResource("/importdata/import_9").getPath();
        executeImportRequest("{\"directory\": \"" + path + "\", \"settings\": true}");

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().filteredIndices("index1");
        IndexMetaData stats = client().admin().cluster().state(clusterStateRequest).actionGet().getState().metaData().index("index1");
        assertEquals(2, stats.numberOfShards());
        assertEquals(1, stats.numberOfReplicas());
    }

    @Test
    public void testSettingsNotFound() {
        String path = getClass().getResource("/importdata/import_1").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\", \"settings\": true}");
        List<Map<String, Object>> failures = getImportFailures(response);
        assertTrue(failures.get(0).get("reason").toString().matches(
                "(.*)Settings file (.*)/importdata/import_1/import_1.json.settings could not be found.(.*)"));
    }

    @Test
    public void testMappingsWithoutIndex() {
        String path = getClass().getResource("/importdata/import_9").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\", \"mappings\": true}");
        List<Map<String, Object>> failures = getImportFailures(response);
        assertEquals(1, failures.size());
        assertTrue(failures.get(0).get("reason").toString().contains("Unable to create mapping. Index index1 missing."));
    }

    @Test
    public void testMappings() {
        createIndex("index1");
        String path = getClass().getResource("/importdata/import_9").getPath();
        executeImportRequest("{\"directory\": \"" + path + "\", \"mappings\": true}");

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().filteredIndices("index1");
        ImmutableMap<String, MappingMetaData> mappings = ImmutableMap.copyOf(
            client().admin().cluster().state(clusterStateRequest).actionGet().getState().metaData().index("index1").getMappings());
        assertEquals("{\"1\":{\"_timestamp\":{\"enabled\":true,\"store\":true},\"_ttl\":{\"enabled\":true,\"default\":86400000},\"properties\":{\"name\":{\"type\":\"string\",\"store\":true}}}}",
                mappings.get("1").source().toString());
    }

    @Test
    public void testMappingNotFound() {
        createIndex("index1");
        String path = getClass().getResource("/importdata/import_1").getPath();
        ImportResponse response = executeImportRequest("{\"directory\": \"" + path + "\", \"mappings\": true}");
        List<Map<String, Object>> failures = getImportFailures(response);
        assertTrue(failures.get(0).get("reason").toString().matches(
                "(.*)Mapping file (.*)/importdata/import_1/import_1.json.mapping could not be found.(.*)"));
    }

    /**
     * Make a subdirectory in each node's node data location.
     * @param directory
     */
    private void makeNodeDataLocationDirectories(String directory) {
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source("{\"output_file\": \"" + directory + "\", \"fields\": [\"_source\", \"_id\", \"_index\", \"_type\"], \"force_overwrite\": true, \"explain\": true}");
        ExportResponse explain = client().execute(ExportAction.INSTANCE, exportRequest).actionGet();

        try {
            Map<String, Object> res = toMap(explain);
            List<Map<String, String>> list = (ArrayList<Map<String, String>>) res.get("exports");
            for (Map<String, String> map : list) {
                new File(map.get("output_file").toString()).mkdir();
            }
        } catch (IOException e) {
        }
    }

    private boolean existsWithField(String id, String field, String value) {
        return existsWithField(id, field, value, "test", "d");
    }

    private boolean existsWithField(String id, String field, String value, String index, String type) {
        GetRequestBuilder rb = new GetRequestBuilder(client(), index);
        GetResponse res = rb.setType(type).setId(id).execute().actionGet();
        return res.isExists() && res.getSourceAsMap().get(field).equals(value);
    }

    private static List<Map<String, Object>> getImports(ImportResponse resp) {
        return get(resp, "imports");
    }

    private static List<Map<String, Object>> getImportFailures(ImportResponse resp) {
        return get(resp, "failures");
    }

    private static List<Map<String, Object>> get(ImportResponse resp, String key) {
        Map<String, Object> res = null;
        try {
            res = toMap(resp);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return (List<Map<String, Object>>) res.get(key);
    }

    private ImportResponse executeImportRequest(String source) {
        ImportRequest request = new ImportRequest();
        request.source(source);
        return client().execute(ImportAction.INSTANCE, request).actionGet();
    }


    /**
     * A file pattern as a path suffix can be specified to filter only for files with a given
     * regex.
     * The other files are not imported.
     */
    @Test
    public void testPathWithFilePattern() {
        String path = getClass().getResource("/importdata/import_8").getPath();
        path = Joiner.on(File.separator).join(path, "index_test_(.*).json");
        ImportResponse response = executeImportRequest("{\"path\": \"" + path + "\"}");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(1, imports.size());
        Map<String, Object> nodeInfo = imports.get(0);
        List imported = (List) nodeInfo.get("imported_files");
        assertTrue(imported.size() == 1);
        assertTrue(imported.get(0).toString().matches(
                "\\{file_name=(.*)/importdata/import_8/index_test_1.json, successes=2, failures=0\\}"));
        assertTrue(existsWithField("802", "name", "802", "test", "d"));
        assertTrue(existsWithField("803", "name", "803", "test", "d"));
        assertFalse(existsWithField("811", "name", "811", "test", "d"));
        assertFalse(existsWithField("812", "name", "812", "test", "d"));
    }

    @Test
    public void testPathWithVars() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("node.name", "import-test-with-path-vars");

        String node3 = cluster().startNode(settingsBuilder);
        client(node3).admin().indices().prepareDelete().execute().actionGet();
        client(node3).admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        String path = getClass().getResource("/importdata/import_10").getPath();
        path = Joiner.on(File.separator).join(path, "import_node-${node}.json");

        ImportRequest request = new ImportRequest();
        request.source("{\"path\": \"" + path + "\"}");
        ImportResponse response =  client(node3).execute(ImportAction.INSTANCE, request).actionGet();

        int successfullyImported = 0;
        List<String> importedFiles = new ArrayList<>();
        for (NodeImportResponse nodeImportResponse : response.getResponses()) {
            for (Importer.ImportCounts importCounts : nodeImportResponse.result().importCounts) {
                successfullyImported += importCounts.successes;
                importedFiles.add(importCounts.fileName);
            }
        }
        assertEquals(1, successfullyImported);

        assertEquals(1, importedFiles.size());
        assertTrue(importedFiles.get(0).matches(
                "(.*)/importdata/import_10/import_node-import-test-with-path-vars.json"));
        assertTrue(existsWithField("1001", "name", "1001", "test", "d"));

        cluster().stopNode(node3);
    }
}
