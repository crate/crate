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

package io.crate.operation.collect.files;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import io.crate.analyze.CopyFromParserProperties;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

public class CSVLineParser {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ArrayList<String> headerKeyList = new ArrayList<>();
    private String[] columnNamesArray;
    private final List<String> targetColumns;
    private final ObjectReader csvReader;

    public CSVLineParser(CopyFromParserProperties properties, List<String> columns) {
        targetColumns = columns;
        if (!properties.fileHeader()) {
            columnNamesArray = new String[targetColumns.size()];
            for (int i = 0; i < targetColumns.size(); i++) {
                columnNamesArray[i] = targetColumns.get(i);
            }
        }
        var mapper = new CsvMapper()
            .enable(CsvParser.Feature.TRIM_SPACES);
        if (properties.emptyStringAsNull()) {
            mapper.enable(CsvParser.Feature.EMPTY_STRING_AS_NULL);
        }
        var csvSchema = mapper
            .typedSchemaFor(String.class)
            .withColumnSeparator(properties.columnSeparator());
        csvReader = mapper
            .readerWithTypedSchemaFor(Object.class)
            .with(csvSchema);
    }

    public void parseHeader(String header) throws IOException {
        MappingIterator<String> iterator = csvReader.readValues(header.getBytes(StandardCharsets.UTF_8));
        iterator.readAll(headerKeyList);
        columnNamesArray = new String[headerKeyList.size()];
        for (int i = 0; i < headerKeyList.size(); i++) {
            String headerKey = headerKeyList.get(i);
            if (targetColumns.isEmpty() || targetColumns.contains(headerKey)) {
                columnNamesArray[i] = headerKey;
            }
        }
        HashSet<String> keySet = new HashSet<>(headerKeyList);
        keySet.remove("");
        if (keySet.size() != headerKeyList.size() || keySet.size() == 0) {
            throw new IllegalArgumentException("Invalid header: duplicate entries or no entries present");
        }
    }

    public byte[] parse(String row, long rowNumber) throws IOException {
        MappingIterator<Object> iterator = csvReader.readValues(row.getBytes(StandardCharsets.UTF_8));
        out.reset();
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.JSON_XCONTENT, out).startObject();
        int i = 0, j = 0;
        while (iterator.hasNext()) {
            if (i >= headerKeyList.size()) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Number of values exceeds " +
                                                                "number of keys in csv file at line %d", rowNumber));
            }
            if (columnNamesArray.length == j || i >= columnNamesArray.length) {
                break;
            }
            var key = columnNamesArray[i];
            var value = iterator.next();
            i++;
            if (key != null) {
                jsonBuilder.field(key, value);
                j++;
            }
        }
        jsonBuilder.endObject().close();
        return out.toByteArray();
    }

    public byte[] parseWithoutHeader(String row, long rowNumber) throws IOException {
        MappingIterator<String> iterator = csvReader.readValues(row.getBytes(StandardCharsets.UTF_8));
        out.reset();
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.JSON_XCONTENT, out).startObject();
        int i = 0;
        while (iterator.hasNext()) {
            if (i >= columnNamesArray.length) {
                break;
            }
            var key = columnNamesArray[i];
            var value = iterator.next();
            i++;
            if (key != null) {
                jsonBuilder.field(key, value);
            }
        }
        if (columnNamesArray.length > i) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Expected %d values, " +
                                               "encountered %d at line %d. This is not allowed when there " +
                                               "is no header provided)",columnNamesArray.length, i, rowNumber));
        }
        jsonBuilder.endObject().close();
        return out.toByteArray();
    }
}
