package org.cratedb.action.import_;

import java.io.File;
import java.util.regex.Pattern;

public class ImportContext {

    private static final String VAR_NODE = "${node}";
    private static final String VAR_INDEX = "${index}";
    private static final String VAR_TABLE = "${table}";
    private static final String VAR_CLUSTER = "${cluster}";

    private String nodePath;
    private boolean compression;
    private String path;
    private Pattern file_pattern;
    private boolean mappings = false;
    private boolean settings = false;
    private String nodeId;
    private String clusterName;
    private String index;

    public ImportContext(String nodePath, String nodeId, String clusterName, String index) {
        this.nodePath = nodePath;
        this.nodeId = nodeId;
        this.clusterName = clusterName;
        this.index = index;
    }

    public boolean compression() {
        return compression;
    }

    public void compression(boolean compression) {
        this.compression = compression;
    }

    public String path() {
        return path;
    }

    public void path(String path) {
        path = applyVars(path);
        File file = new File(path);
        if (!file.isAbsolute() && nodePath != null) {
            file = new File(nodePath, path);
            path = file.getAbsolutePath();
        }
        this.path = path;
    }

    public Pattern file_pattern() {
        return file_pattern;
    }

    public void file_pattern(Pattern file_pattern) {
        this.file_pattern = file_pattern;
    }

    public boolean mappings() {
        return mappings;
    }

    public void mappings(boolean mappings) {
        this.mappings = mappings;
    }

    public boolean settings() {
        return settings;
    }

    public void settings(boolean settings) {
        this.settings = settings;
    }

    /**
     * Replaces variable placeholder with actual value
     *
     * @param template
     * @return
     */
    private String applyVars(String template) {
        template = template.replace(VAR_NODE, nodeId);
        if (index != null) {
            template = template.replace(VAR_INDEX, index);
            template = template.replace(VAR_TABLE, index);
        }
        template = template.replace(VAR_CLUSTER, clusterName);
        return template;
    }

}
