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

package io.crate.server.xcontent;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

@SuppressWarnings("unchecked")
public class XContentHelper {

    /**
     * Creates a parser for the bytes using the supplied content-type
     */
    public static XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler,
                                              BytesReference bytes, XContentType xContentType) throws IOException {
        Objects.requireNonNull(xContentType);
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = compressor.threadLocalInputStream(bytes.streamInput());
            if (compressedInput.markSupported() == false) {
                compressedInput = new BufferedInputStream(compressedInput);
            }
            return xContentType.xContent().createParser(xContentRegistry, deprecationHandler, compressedInput);
        } else {
            return xContentType.xContent().createParser(xContentRegistry, deprecationHandler, bytes.streamInput());
        }
    }

    public static Map<String, Object> toMap(BytesReference bytes, XContentType xContentType) {
        try (InputStream inputStream = getUncompressedInputStream(bytes)) {
            XContentParser parser = xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                inputStream
            );
            return parser.map();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static InputStream getUncompressedInputStream(BytesReference bytes) throws IOException {
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor == null) {
            return bytes.streamInput();
        }
        InputStream compressedStreamInput = compressor.threadLocalInputStream(bytes.streamInput());
        if (compressedStreamInput.markSupported()) {
            return compressedStreamInput;
        } else {
            return new BufferedInputStream(compressedStreamInput);
        }
    }

    /**
     * Converts the given bytes into a map that is optionally ordered.
     */
    public static ParsedXContent convertToMap(BytesReference bytes, boolean ordered, XContentType xContentType)
        throws ElasticsearchParseException {
        try {
            try (InputStream input = getUncompressedInputStream(bytes)) {
                return new ParsedXContent(
                    xContentType,
                    convertToMap(xContentType.xContent(), input, ordered));
            }
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link ElasticsearchParseException} if there is any
     * error.
     */
    public static Map<String, Object> convertToMap(XContent xContent, String string, boolean ordered) throws ElasticsearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, string)) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link ElasticsearchParseException} if there is any
     * error. This doesn't automatically uncompress the input.
     */
    public static Map<String, Object> convertToMap(XContent xContent, InputStream input, boolean ordered)
            throws ElasticsearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Updates the provided changes into the source. If the key exists in the changes, it overrides the one in source
     * unless both are Maps, in which case it recursively updated it.
     *
     * @param source                 the original map to be updated
     * @param changes                the changes to update into updated
     * @param checkUpdatesAreUnequal should this method check if updates to the same key (that are not both maps) are
     *                               unequal?  This is just a .equals check on the objects, but that can take some time on long strings.
     * @return true if the source map was modified
     */
    public static boolean update(Map<String, Object> source, Map<String, Object> changes, boolean checkUpdatesAreUnequal) {
        boolean modified = false;
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            if (!source.containsKey(changesEntry.getKey())) {
                // safe to copy, change does not exist in source
                source.put(changesEntry.getKey(), changesEntry.getValue());
                modified = true;
                continue;
            }
            Object old = source.get(changesEntry.getKey());
            if (old instanceof Map && changesEntry.getValue() instanceof Map) {
                // recursive merge maps
                modified |= update((Map<String, Object>) source.get(changesEntry.getKey()),
                        (Map<String, Object>) changesEntry.getValue(), checkUpdatesAreUnequal && !modified);
                continue;
            }
            // update the field
            source.put(changesEntry.getKey(), changesEntry.getValue());
            if (modified) {
                continue;
            }
            if (!checkUpdatesAreUnequal) {
                modified = true;
                continue;
            }
            modified = !Objects.equals(old, changesEntry.getValue());
        }
        return modified;
    }

    /**
     * Merges the defaults provided as the second parameter into the content of the first. Only does recursive merge
     * for inner maps.
     */
    public static void mergeDefaults(Map<String, Object> content, Map<String, Object> defaults) {
        for (Map.Entry<String, Object> defaultEntry : defaults.entrySet()) {
            if (!content.containsKey(defaultEntry.getKey())) {
                // copy it over, it does not exists in the content
                content.put(defaultEntry.getKey(), defaultEntry.getValue());
            } else {
                // in the content and in the default, only merge compound ones (maps)
                if (content.get(defaultEntry.getKey()) instanceof Map && defaultEntry.getValue() instanceof Map) {
                    mergeDefaults((Map<String, Object>) content.get(defaultEntry.getKey()), (Map<String, Object>) defaultEntry.getValue());
                } else if (content.get(defaultEntry.getKey()) instanceof List && defaultEntry.getValue() instanceof List) {
                    List<Object> defaultList = (List<Object>) defaultEntry.getValue();
                    List<Object> contentList = (List<Object>) content.get(defaultEntry.getKey());

                    if (allListValuesAreMapsOfOne(defaultList) && allListValuesAreMapsOfOne(contentList)) {
                        // all are in the form of [ {"key1" : {}}, {"key2" : {}} ], merge based on keys
                        Map<String, Map<String, Object>> processed = new LinkedHashMap<>();
                        for (Object o : contentList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            processed.put(entry.getKey(), map);
                        }
                        for (Object o : defaultList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            if (processed.containsKey(entry.getKey())) {
                                mergeDefaults(processed.get(entry.getKey()), map);
                            } else {
                                // put the default entries after the content ones.
                                processed.put(entry.getKey(), map);
                            }
                        }

                        content.put(defaultEntry.getKey(), new ArrayList<>(processed.values()));
                    } else {
                        // if both are lists, simply combine them, first the defaults, then the content
                        // just make sure not to add the same value twice
                        List<Object> mergedList = new ArrayList<>(defaultList);

                        for (Object o : contentList) {
                            if (!mergedList.contains(o)) {
                                mergedList.add(o);
                            }
                        }
                        content.put(defaultEntry.getKey(), mergedList);
                    }
                }
            }
        }
    }

    private static boolean allListValuesAreMapsOfOne(List<Object> list) {
        for (Object o : list) {
            if (!(o instanceof Map)) {
                return false;
            }
            if (((Map<?, ?>) o).size() != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(ToXContent toXContent, XContentType xContentType, Params params, boolean humanReadable) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.humanReadable(humanReadable);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return BytesReference.bytes(builder);
        }
    }
}
