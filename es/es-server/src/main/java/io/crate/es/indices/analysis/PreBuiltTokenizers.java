/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */
package io.crate.es.indices.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import io.crate.es.Version;
import io.crate.es.index.analysis.TokenFilterFactory;
import io.crate.es.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

public enum PreBuiltTokenizers {

    STANDARD(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new StandardTokenizer();
        }
    }

    ;

    protected abstract  Tokenizer create(Version version);

    protected TokenFilterFactory getMultiTermComponent(Version version) {
        return null;
    }

    private final CachingStrategy cachingStrategy;

    PreBuiltTokenizers(CachingStrategy cachingStrategy) {
        this.cachingStrategy = cachingStrategy;
    }

    public CachingStrategy getCachingStrategy() {
        return cachingStrategy;
    }
}
