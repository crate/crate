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
import com.google.common.primitives.Ints;
import io.crate.types.DataTypes;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SummitsIterable implements Supplier<Iterable<?>> {

    private final Supplier<List<SummitsContext>> summitsSupplierCache
        = Suppliers.memoizeWithExpiration(this::fetchSummits, 4, TimeUnit.MINUTES);

    private List<SummitsContext> fetchSummits() {
        try {
            Path path = Paths.get(
                InternalSettingsPreparer.class.getResource("/config/names.txt").toURI().getPath()
            );
            return Files.lines(path)
                .map(line -> {
                    String[] parts = line.split("\t", -1);
                    return new SummitsContext(parts[0],
                        Ints.tryParse(parts[1]), Ints.tryParse(parts[2]),
                        safeParseCoordinates(parts[3]),
                        parts[4], parts[5], parts[6], parts[7],
                        Ints.tryParse(parts[8])
                    );
                })
                .collect(Collectors.toList());
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Cannot populate the sys.summits table", e);
        }
    }

    private Double[] safeParseCoordinates(String value) {
        return value.isEmpty() ? null : DataTypes.GEO_POINT.value(value);
    }

    @Override
    public Iterable<?> get() {
        return summitsSupplierCache.get();
    }
}
