/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.assertj.core.api.AbstractAssert;
import org.elasticsearch.index.engine.Engine;

public class EngineSearcherAssert extends AbstractAssert<EngineSearcherAssert, Engine.Searcher> {


    protected EngineSearcherAssert(Engine.Searcher actual) {
        super(actual, EngineSearcherAssert.class);
    }

    public EngineSearcherAssert hasTotalHits(int totalHits) {
        return hasTotalHits(new MatchAllDocsQuery(), totalHits);
    }

    public EngineSearcherAssert hasTotalHits(Query query, int totalHits) {
        describedAs("total hits of size " + totalHits + " with query " + query);
        try {
            assertThat(actual.count(query)).isEqualTo(totalHits);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        return this;
    }
}

