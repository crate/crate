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

package io.crate.action.export;

import io.crate.export.Output;
import io.crate.export.OutputCommand;
import io.crate.export.OutputFile;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Container class for export specific informations.
 */
public class ExportContext extends DefaultSearchContext {

    private static final String VAR_SHARD = "${shard}";
    private static final String VAR_INDEX = "${index}";
    private static final String VAR_CLUSTER = "${cluster}";

    private List<String> outputCmdArray;
    private String outputCmd;
    private String outputFile;
    private boolean forceOverride = false;
    private boolean compression;
    private String nodePath;
    private boolean mappings = false;
    private boolean settings = false;

    public ExportContext(long id, ShardSearchRequest request, SearchShardTarget shardTarget,
                         Engine.Searcher engineSearcher, IndexService indexService, IndexShard indexShard,
                         ScriptService scriptService, CacheRecycler cacheRecycler,
                         PageCacheRecycler pageRecycler, BigArrays bigArrays, String nodePath) {
        super(id, request, shardTarget, engineSearcher, indexService, indexShard, scriptService,
                cacheRecycler, pageRecycler, bigArrays);
        this.nodePath = nodePath;
    }

    public List<String> outputCmdArray() {
        return outputCmdArray;
    }

    public void outputCmdArray(List<String> outputCmdArray) {
        this.outputCmdArray = applyVars(outputCmdArray);
    }

    public String outputCmd() {
        return outputCmd;
    }

    public void outputCmd(String outputCmd) {
        this.outputCmd = applyVars(outputCmd);
    }

    public String outputFile() {
        return outputFile;
    }

    public void outputFile(String outputFile) {
        outputFile = applyVars(outputFile);
        File outFile = new File(outputFile);
        if (!outFile.isAbsolute() && nodePath != null) {
            outputFile = new File(nodePath, outputFile).getAbsolutePath();
        }
        this.outputFile = outputFile;
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

    public String nodePath() {
        return nodePath;
    }

    public boolean forceOverride() {
        return forceOverride;
    }

    public void forceOverride(boolean forceOverride) {
        this.forceOverride = forceOverride;
    }

    public void compression(boolean compression) {
        this.compression = compression;
    }

    public boolean compression() {
        return this.compression;
    }

    /**
     * Replaces variable placeholder with actual value in all elements of templateArray
     *
     * @param templateArray
     * @return
     */
    private List<String> applyVars(List<String> templateArray) {
        List<String> ret = new ArrayList<String>();
        for (String part : templateArray) {
            ret.add(applyVars(part));
        }
        return ret;
    }

    /**
     * Replaces variable placeholder with actual value
     *
     * @param template
     * @return
     */
    private String applyVars(String template) {
        template = template.replace(VAR_SHARD, String.valueOf(indexShard().shardId().getId()));
        template = template.replace(VAR_INDEX, indexShard().shardId().getIndex());
        template = template.replace(VAR_CLUSTER, clusterName());
        return template;
    }

    /**
     * Method to retrieve name of cluster
     *
     * @return name of cluster
     */
    private String clusterName() {
        return ClusterName.clusterNameFromSettings(this.indexShard().indexSettings()).value();
    }

    public Output createOutput() {
        if (outputFile()!=null){
            return new OutputFile(outputFile(), forceOverride(), compression);
        } else {
            if (outputCmd()!=null){
                return new OutputCommand(outputCmd(), compression);
            } else {
                return new OutputCommand(outputCmdArray(), compression);
            }
        }
    }
}
