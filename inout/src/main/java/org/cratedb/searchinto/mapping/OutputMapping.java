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

package org.cratedb.searchinto.mapping;

import org.elasticsearch.search.SearchHit;

public class OutputMapping {

    private final String srcName;
    private final String trgName;
    private final String srcLiteral;
    private final FieldReader reader;
    private final FieldWriter writer;
    private SearchHit hit;


    private static String getLiteral(String candidate) {
        if (candidate != null && candidate.length() > 2) {
            if ((candidate.startsWith("'") && candidate.endsWith(
                    "'")) || (candidate.startsWith("\"") && candidate.endsWith(
                    "\""))) {
                return candidate.substring(1, candidate.length() - 1);
            }
        }
        return null;
    }

    public OutputMapping(String srcName, String trgName) {
        this.srcName = srcName;
        this.trgName = trgName;
        srcLiteral = getLiteral(srcName);
        if (srcLiteral == null) {
            this.reader = new FieldReader(srcName);
        } else {
            this.reader = null;
        }
        this.writer = new FieldWriter(trgName);
    }

    public void setHit(SearchHit hit) {
        this.hit = hit;
    }

    public IndexRequestBuilder toRequestBuilder(IndexRequestBuilder builder) {
        if (srcLiteral != null) {
            writer.setValue(srcLiteral);
        } else {
            reader.setHit(hit);
            writer.setValue(reader.getValue());

        }
        writer.toRequestBuilder(builder);
        return builder;
    }

}
