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

package io.crate.operation.collect.files;

import com.google.common.base.Splitter;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SqlFeaturesIterable implements Iterable {

    private final File features;

    @Inject
    public SqlFeaturesIterable() {
        URL resource = SqlFeaturesIterator.class.getResource("/sql_features.tsv");
        assert resource != null;
        features = new File(resource.getFile());
    }

    @Override
    public Iterator iterator() {
        return new SqlFeaturesIterator();
    }

    private class SqlFeaturesIterator implements Iterator<SqlFeatureContext> {

        private final BufferedReader reader;
        private String nextLine;

        SqlFeaturesIterator() {
            BufferedReader tmp;
            try {
                tmp = Files.newBufferedReader(features.toPath(), Charset.forName("UTF-8"));
            } catch (IOException e) {
                tmp = null;
            }
            reader = tmp;
            if (reader != null) {
                try {
                    nextLine = tmp.readLine();
                } catch (IOException e) {
                    nextLine = null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (nextLine != null) {
                return true;
            } else {
                try {
                    nextLine = reader.readLine();
                    return (nextLine != null);
                } catch (IOException e) {
                    return false;
                }
            }
        }

        @Override
        public SqlFeatureContext next() {
            String next = nextLine();
            SqlFeatureContext ctx = null;
            if (next != null) {
                List<String> parts = Splitter.on("\t").splitToList(next);
                ctx = SqlFeatureContext.builder()
                    .featureId(parts.get(0))
                    .featureName(parts.get(1))
                    .subFeatureId(parts.get(2))
                    .subFeatureName(parts.get(3))
                    .isSupported(parts.get(4).equals("YES"))
                    .isVerifiedBy(parts.get(5).isEmpty() ? null : parts.get(5))
                    .comments(parts.get(6).isEmpty() ? null : parts.get(6))
                    .build();
            }
            return ctx;
        }

        @Nullable
        private String nextLine() {
            if (nextLine != null || hasNext()) {
                String line = nextLine;
                nextLine = null;
                return line;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}
