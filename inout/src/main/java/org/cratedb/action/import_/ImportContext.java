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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
    private String nodeName;
    private String clusterName;
    private String index;

    public ImportContext(String nodePath, String nodeName, String clusterName, String index) {
        this.nodePath = nodePath;
        this.nodeName = nodeName;
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
        template = template.replace(VAR_NODE, nodeName);
        if (index != null) {
            template = template.replace(VAR_INDEX, index);
            template = template.replace(VAR_TABLE, index);
        }
        template = template.replace(VAR_CLUSTER, clusterName);
        return template;
    }

}
