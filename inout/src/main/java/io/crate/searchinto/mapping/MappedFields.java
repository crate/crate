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

package io.crate.searchinto.mapping;

import io.crate.action.searchinto.SearchIntoContext;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MappedFields {

    private final SearchIntoContext context;
    private SearchHit hit;
    private final List<OutputMapping> outputMappings;

    public MappedFields(SearchIntoContext context) {
        this.context = context;
        this.outputMappings = getOutputMappings();
    }

    public void hit(SearchHit hit) {
        this.hit = hit;
    }

    private List<OutputMapping> getOutputMappings() {
        List<OutputMapping> oms = new ArrayList<OutputMapping>(
                context.outputNames().size());
        boolean indexDefined = false;
        boolean typeDefined = false;
        for (Map.Entry<String, String> e : context.outputNames().entrySet()) {
            String srcName = e.getKey();
            String trgName = e.getValue();
            assert (trgName != null);
            if (trgName.equals("_index")) {
                indexDefined = true;
            } else if (trgName.equals("_type")) {
                typeDefined = true;
            }
            OutputMapping om = new OutputMapping(srcName, trgName);
            oms.add(om);
        }
        if (!indexDefined) {
            oms.add(new OutputMapping("_index", "_index"));
        }
        if (!typeDefined) {
            oms.add(new OutputMapping("_type", "_type"));
        }

        return oms;
    }

    public IndexRequest newIndexRequest() {
        IndexRequestBuilder builder = new IndexRequestBuilder();
        for (OutputMapping om : outputMappings) {
            om.setHit(hit);
            builder = om.toRequestBuilder(builder);
        }
        return builder.build();

    }
}

