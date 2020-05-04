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

package io.crate.operation.collect.files;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;

public class CSVLineParser {

    private final ArrayList<String> keyList = new ArrayList<>();
    private final ObjectReader csvReader = new CsvMapper()
        .enable(CsvParser.Feature.TRIM_SPACES)
        .readerWithTypedSchemaFor(String.class);
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public void parseHeader(String header) throws IOException {
        MappingIterator<String> iterator = csvReader.readValues(header.getBytes(StandardCharsets.UTF_8));
        iterator.readAll(keyList);
        HashSet<String> keySet = new HashSet<>(keyList);
        keySet.remove("");
        if (keySet.size() != keyList.size() || keySet.size() == 0) {
            throw new IllegalArgumentException("Invalid header: duplicate entries or no entries present");
        }
    }

    public byte[] parse(String row) throws IOException {
        MappingIterator<Object> iterator = csvReader.readValues(row.getBytes(StandardCharsets.UTF_8));
        out.reset();
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.JSON_XCONTENT, out).startObject();
        int i = 0;
        while (iterator.hasNext()) {
            if (i >= keyList.size()) {
                throw new IllegalArgumentException("Number of values exceeds number of keys");
            }
            jsonBuilder.field(keyList.get(i), iterator.next());
            i++;
        }
        jsonBuilder.endObject().close();
        return out.toByteArray();
    }

}
