/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;

import io.crate.common.collections.Tuple;

/**
 * Encapsulates the  {@link IndexMetadata} instances of a concrete index or indices an alias is pointing to.
 */
public interface AliasOrIndex {

    /**
     * @return whether this an alias or concrete index
     */
    boolean isAlias();

    /**
     * @return All {@link IndexMetadata} of all concrete indices this alias is referring to or if this is a concrete index its {@link IndexMetadata}
     */
    List<IndexMetadata> getIndices();

    /**
     * Represents an concrete index and encapsulates its {@link IndexMetadata}
     */
    class Index implements AliasOrIndex {

        private final IndexMetadata concreteIndex;

        public Index(IndexMetadata indexMetadata) {
            this.concreteIndex = indexMetadata;
        }

        @Override
        public boolean isAlias() {
            return false;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return Collections.singletonList(concreteIndex);
        }

        /**
         * @return If this is an concrete index, its {@link IndexMetadata}
         */
        public IndexMetadata getIndex() {
            return concreteIndex;
        }

    }

    /**
     * Represents an alias and groups all {@link IndexMetadata} instances sharing the same alias name together.
     */
    class Alias implements AliasOrIndex {

        private final String aliasName;
        private final List<IndexMetadata> referenceIndexMetadatas;
        private SetOnce<IndexMetadata> writeIndex = new SetOnce<>();

        public Alias(AliasMetadata aliasMetadata, IndexMetadata indexMetadata) {
            this.aliasName = aliasMetadata.getAlias();
            this.referenceIndexMetadatas = new ArrayList<>();
            this.referenceIndexMetadatas.add(indexMetadata);
        }

        @Override
        public boolean isAlias() {
            return true;
        }

        public String getAliasName() {
            return aliasName;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return referenceIndexMetadatas;
        }


        @Nullable
        public IndexMetadata getWriteIndex() {
            return writeIndex.get();
        }

        /**
         * Returns the unique alias metadata per concrete index.
         *
         * (note that although alias can point to the same concrete indices, each alias reference may have its own routing
         * and filters)
         */
        public Iterable<Tuple<String, AliasMetadata>> getConcreteIndexAndAliasMetadatas() {
            return new Iterable<Tuple<String, AliasMetadata>>() {
                @Override
                public Iterator<Tuple<String, AliasMetadata>> iterator() {
                    return new Iterator<Tuple<String, AliasMetadata>>() {

                        int index = 0;

                        @Override
                        public boolean hasNext() {
                            return index < referenceIndexMetadatas.size();
                        }

                        @Override
                        public Tuple<String, AliasMetadata> next() {
                            IndexMetadata indexMetadata = referenceIndexMetadatas.get(index++);
                            return new Tuple<>(indexMetadata.getIndex().getName(), indexMetadata.getAliases().get(aliasName));
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }

                    };
                }
            };
        }

        public AliasMetadata getFirstAliasMetadata() {
            return referenceIndexMetadatas.get(0).getAliases().get(aliasName);
        }

        void addIndex(IndexMetadata indexMetadata) {
            this.referenceIndexMetadatas.add(indexMetadata);
        }

        public void computeAndValidateWriteIndex() {
            List<IndexMetadata> writeIndices = new ArrayList<>();
            if (writeIndices.isEmpty() && referenceIndexMetadatas.size() == 1) {
                writeIndices.add(referenceIndexMetadatas.get(0));
            }
            if (writeIndices.size() == 1) {
                writeIndex.set(writeIndices.get(0));
            } else if (writeIndices.size() > 1) {
                List<String> writeIndicesStrings = writeIndices.stream()
                    .map(i -> i.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException("alias [" + aliasName + "] has more than one write index [" +
                    Strings.collectionToCommaDelimitedString(writeIndicesStrings) + "]");
            }
        }
    }
}
