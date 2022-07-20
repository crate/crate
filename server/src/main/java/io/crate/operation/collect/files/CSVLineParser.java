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

import io.crate.execution.engine.collect.files.FileReadingIterator.ReusableJsonBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

public class CSVLineParser {

    private final ReusableJsonBuilder reusableJsonBuilder;
    private final ArrayList<String> headerKeyList = new ArrayList<>();
    private String[] columnNamesArray;
    private final List<String> targetColumns;
    private final ObjectReader csvReader;

    public CSVLineParser(CopyFromParserProperties properties, List<String> columns, ReusableJsonBuilder reusableRowBuilder) {
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

        // We re-use XContentBuilder per line via startObject ... add fields ... flush
        // but we cannot immediately set parsed value to a json field.
        // In case parsing fails in the middle we will end up with "dirty" XContentBuilder which we cannot rollback.
        // We first gather values into ArrayList with adjusted once capacity and start making json object only when we know that line was parsed without errors.
        this.reusableJsonBuilder = reusableRowBuilder;
        this.reusableJsonBuilder.rowItems().ensureCapacity(targetColumns.size());
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
        reset();
        MappingIterator<Object> iterator = csvReader.readValues(row.getBytes(StandardCharsets.UTF_8));
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
                reusableJsonBuilder.rowItems().add(value);
                j++;
            }
        }
        return getByteArray();
    }

    public byte[] parseWithoutHeader(String row, long rowNumber) throws IOException {
        MappingIterator<String> iterator = csvReader.readValues(row.getBytes(StandardCharsets.UTF_8));
        reset();
        int i = 0;
        while (iterator.hasNext()) {
            if (i >= columnNamesArray.length) {
                break;
            }
            var key = columnNamesArray[i];
            var value = iterator.next();
            i++;
            if (key != null) {
                reusableJsonBuilder.rowItems().add(value);
            }
        }
        if (columnNamesArray.length > i) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Expected %d values, " +
                                               "encountered %d at line %d. This is not allowed when there " +
                                               "is no header provided)",columnNamesArray.length, i, rowNumber));
        }
        return getByteArray();
    }

    private void reset() throws IOException {
        reusableJsonBuilder.out().reset();
        reusableJsonBuilder.rowItems().clear();
    }

    /**
     * ReusableJsonBuilder.rowItems().get(i) corresponds to columnNamesArray[i-th not-null]
     * We can also store 2 lists (key-value) in reusableJsonBuilder but as we anyway have to iterate over
     * reusableJsonBuilder.rowItems() which in most cases has same size with columnNamesArray we can resolve it on the fly.
     */
    private byte[] getByteArray() throws IOException {
        int notNullCounter = 0;
        reusableJsonBuilder.jsonBuilder().startObject();
        for (int i = 0; i < columnNamesArray.length; i++) {
            var key = columnNamesArray[i];
            if (key != null) {
                reusableJsonBuilder.jsonBuilder().field(key, reusableJsonBuilder.rowItems().get(notNullCounter));
                notNullCounter++;
            }
        }
        reusableJsonBuilder.jsonBuilder().endObject().flush();
        return reusableJsonBuilder.out().toByteArray();
    }
}
