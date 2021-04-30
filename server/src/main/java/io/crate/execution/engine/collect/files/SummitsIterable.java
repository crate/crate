/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect.files;

import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import io.crate.types.DataTypes;
import org.locationtech.spatial4j.shape.Point;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SummitsIterable implements Iterable<SummitsContext> {

    private static final Splitter TAB_SPLITTER = Splitter.on("\t");

    private final Supplier<List<SummitsContext>> summitsSupplierCache = Suppliers.memoizeWithExpiration(
        this::fetchSummits, 4, TimeUnit.MINUTES
    );

    private List<SummitsContext> fetchSummits() {
        List<SummitsContext> summits = new ArrayList<>();
        try (InputStream input = SummitsIterable.class.getResourceAsStream("/config/names.txt")) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    List<String> parts = TAB_SPLITTER.splitToList(line);
                    summits.add(new SummitsContext(
                        parts.get(0),
                        Ints.tryParse(parts.get(1)),
                        Ints.tryParse(parts.get(2)),
                        safeParseCoordinates(parts.get(3)),
                        parts.get(4),
                        parts.get(5),
                        parts.get(6),
                        parts.get(7),
                        Ints.tryParse(parts.get(8)))
                    );
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot populate the sys.summits table", e);
        }
        return summits;
    }

    @Nullable
    private static Point safeParseCoordinates(String value) {
        return value.isEmpty() ? null : DataTypes.GEO_POINT.implicitCast(value);
    }

    @Override
    public Iterator<SummitsContext> iterator() {
        return summitsSupplierCache.get().iterator();
    }
}
