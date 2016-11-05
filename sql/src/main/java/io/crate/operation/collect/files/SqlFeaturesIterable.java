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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SqlFeaturesIterable implements Supplier<Iterable<?>> {

    private final Supplier<List<SqlFeatureContext>> summitsSupplierCache
        = Suppliers.memoizeWithExpiration(this::fetchSqlFeatures, 4, TimeUnit.MINUTES);

    private List<SqlFeatureContext> fetchSqlFeatures() {
        try {
            Path path = Paths.get(
                InternalSettingsPreparer.class.getResource("/sql_features.tsv").toURI().getPath()
            );
            return Files.lines(path)
                .map(line -> {
                    List<String> parts = Arrays.asList(line.split("\t", -1));
                    return new SqlFeatureContext(
                        parts.get(0), parts.get(1), parts.get(2), parts.get(3),
                        parts.get(4).equals("YES"),
                        parts.get(5).isEmpty() ? null : parts.get(5),
                        parts.get(6).isEmpty() ? null : parts.get(6)
                    );
                })
                .collect(Collectors.toList());
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Cannot populate the information_schema.sql_features table", e);
        }
    }

    @Override
    public Iterable<?> get() {
        return summitsSupplierCache.get();
    }
}
