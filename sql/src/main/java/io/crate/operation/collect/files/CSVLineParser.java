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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CSVLineParser {

    private List<Object> keyList;
    private ObjectMapper objectMapper = new ObjectMapper();
    private CsvObjectReader csvReader = new CsvMapper().enable(CsvParser.Feature.TRIM_SPACES)
        .readerWithTypedSchemaFor(String.class);

    public void parseHeader(BufferedReader currentReader) throws IOException {
        String header = currentReader.readLine();
        keyList = csvReader.readValues(header.getBytes(StandardCharsets.UTF_8)).readAll();
        Set<Object> keySet = new HashSet<>(keyList);
        keySet.remove("");

        if (keySet.size() != keyList.size() || keySet.size() == 0) {
            throw new IllegalArgumentException("Invalid header: duplicate entries or no entries present");
        }
    }

    public byte[] parse(String row) throws IOException {
        MappingIterator<Object> iterator = csvReader.readValues(row.getBytes(StandardCharsets.UTF_8));
        HashMap<Object, Object> csvAsMap = new HashMap<>();
        int i = 0;
        while (iterator.hasNext()) {
            if (iterator.hasNext() && i >= keyList.size()) {
                throw new IllegalArgumentException("Number of values exceeds number of keys");
            }

            csvAsMap.put(keyList.get(i), iterator.next());
            i++;
        }
        return objectMapper.writeValueAsBytes(csvAsMap);
    }

}
