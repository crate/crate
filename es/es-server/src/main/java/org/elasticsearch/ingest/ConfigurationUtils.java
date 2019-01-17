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

package org.elasticsearch.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;

public final class ConfigurationUtils {

    public static final String TAG_KEY = "tag";

    private ConfigurationUtils() {
    }

    /**
     * Returns and removes the specified optional property from the specified configuration map.
     *
     * If the property value isn't of type string a {@link ElasticsearchParseException} is thrown.
     */
    public static String readOptionalStringProperty(String processorType, String processorTag,
                                                    Map<String, Object> configuration, String propertyName) {
        Object value = configuration.remove(propertyName);
        return readString(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string an {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static String readStringProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                            String propertyName) {
        return readStringProperty(processorType, processorTag, configuration, propertyName, null);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string a {@link ElasticsearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link ElasticsearchParseException} is thrown
     */
    public static String readStringProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                            String propertyName, String defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return readString(processorType, processorTag, propertyName, value);
    }

    private static String readString(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw newConfigurationException(processorType, processorTag, propertyName, "property isn't a string, but of type [" +
            value.getClass().getName() + "]");
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string or int a {@link ElasticsearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link ElasticsearchParseException} is thrown
     */
     public static String readStringOrIntProperty(String processorType, String processorTag,
            Map<String, Object> configuration, String propertyName, String defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "required property is missing");
        }
        return readStringOrInt(processorType, processorTag, propertyName, value);
    }

    private static String readStringOrInt(String processorType, String processorTag,
            String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer) {
            return String.valueOf(value);
        }
        throw newConfigurationException(processorType, processorTag, propertyName,
            "property isn't a string or int, but of type [" + value.getClass().getName() + "]");
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string or int a {@link ElasticsearchParseException} is thrown.
     */
    public static String readOptionalStringOrIntProperty(String processorType, String processorTag,
            Map<String, Object> configuration, String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }
        return readStringOrInt(processorType, processorTag, propertyName, value);
    }

    public static Boolean readBooleanProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                             String propertyName, boolean defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return defaultValue;
        } else {
            return readBoolean(processorType, processorTag, propertyName, value).booleanValue();
        }
    }

    private static Boolean readBoolean(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        throw newConfigurationException(processorType, processorTag, propertyName, "property isn't a boolean, but of type [" +
            value.getClass().getName() + "]");
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type int a {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static Integer readIntProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                          String propertyName, Integer defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "property cannot be converted to an int [" + value.toString() + "]");
        }
    }

    /**
     * Returns and removes the specified property of type list from the specified configuration map.
     *
     * If the property value isn't of type list an {@link ElasticsearchParseException} is thrown.
     */
    public static <T> List<T> readOptionalList(String processorType, String processorTag, Map<String, Object> configuration,
                                               String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }
        return readList(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property of type list from the specified configuration map.
     *
     * If the property value isn't of type list an {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static <T> List<T> readList(String processorType, String processorTag, Map<String, Object> configuration,
                                       String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }

        return readList(processorType, processorTag, propertyName, value);
    }

    private static <T> List<T> readList(String processorType, String processorTag, String propertyName, Object value) {
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<T> stringList = (List<T>) value;
            return stringList;
        } else {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "property isn't a list, but of type [" + value.getClass().getName() + "]");
        }
    }

    /**
     * Returns and removes the specified property of type map from the specified configuration map.
     *
     * If the property value isn't of type map an {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static <T> Map<String, T> readMap(String processorType, String processorTag, Map<String, Object> configuration,
                                             String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }

        return readMap(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property of type map from the specified configuration map.
     *
     * If the property value isn't of type map an {@link ElasticsearchParseException} is thrown.
     */
    public static <T> Map<String, T> readOptionalMap(String processorType, String processorTag, Map<String, Object> configuration,
                                                     String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }

        return readMap(processorType, processorTag, propertyName, value);
    }

    private static <T> Map<String, T> readMap(String processorType, String processorTag, String propertyName, Object value) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, T> map = (Map<String, T>) value;
            return map;
        } else {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "property isn't a map, but of type [" + value.getClass().getName() + "]");
        }
    }

    /**
     * Returns and removes the specified property as an {@link Object} from the specified configuration map.
     */
    public static Object readObject(String processorType, String processorTag, Map<String, Object> configuration,
                                    String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return value;
    }

    public static ElasticsearchException newConfigurationException(String processorType, String processorTag,
                                                                        String propertyName, String reason) {
        String msg;
        if (propertyName == null) {
           msg = reason;
        } else {
            msg = "[" + propertyName + "] " + reason;
        }
        ElasticsearchParseException exception = new ElasticsearchParseException(msg);
        addHeadersToException(exception, processorType, processorTag, propertyName);
        return exception;
    }

    public static ElasticsearchException newConfigurationException(String processorType, String processorTag,
                                                                        String propertyName, Exception cause) {
        ElasticsearchException exception = ExceptionsHelper.convertToElastic(cause);
        addHeadersToException(exception, processorType, processorTag, propertyName);
        return exception;
    }

    public static List<Processor> readProcessorConfigs(List<Map<String, Object>> processorConfigs,
                                                       ScriptService scriptService,
                                                       Map<String, Processor.Factory> processorFactories) throws Exception {
        Exception exception = null;
        List<Processor> processors = new ArrayList<>();
        if (processorConfigs != null) {
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    try {
                        processors.add(readProcessor(processorFactories, scriptService, entry.getKey(), entry.getValue()));
                    } catch (Exception e) {
                        exception = ExceptionsHelper.useOrSuppress(exception, e);
                    }
                }
            }
        }

        if (exception != null) {
            throw exception;
        }

        return processors;
    }

    public static TemplateScript.Factory compileTemplate(String processorType, String processorTag, String propertyName,
                                                           String propertyValue, ScriptService scriptService) {
        try {
            // This check is here because the DEFAULT_TEMPLATE_LANG(mustache) is not
            // installed for use by REST tests. `propertyValue` will not be
            // modified if templating is not available so a script that simply returns an unmodified `propertyValue`
            // is returned.
            if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG)) {
                Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, propertyValue, Collections.emptyMap());
                return scriptService.compile(script, TemplateScript.CONTEXT);
            } else {
                return (params) -> new TemplateScript(params) {
                    @Override
                    public String execute() {
                        return propertyValue;
                    }
                };
            }
        } catch (Exception e) {
            throw ConfigurationUtils.newConfigurationException(processorType, processorTag, propertyName, e);
        }
    }

    private static void addHeadersToException(ElasticsearchException exception, String processorType,
                                              String processorTag, String propertyName) {
        if (processorType != null) {
            exception.addHeader("processor_type", processorType);
        }
        if (processorTag != null) {
            exception.addHeader("processor_tag", processorTag);
        }
        if (propertyName != null) {
            exception.addHeader("property_name", propertyName);
        }
    }

    @SuppressWarnings("unchecked")
    public static Processor readProcessor(Map<String, Processor.Factory> processorFactories,
                                          ScriptService scriptService,
                                          String type, Object config) throws Exception {
        if (config instanceof Map) {
            return readProcessor(processorFactories, scriptService, type, (Map<String, Object>) config);
        } else if (config instanceof String && "script".equals(type)) {
            Map<String, Object> normalizedScript = new HashMap<>(1);
            normalizedScript.put(ScriptType.INLINE.getParseField().getPreferredName(), config);
            return readProcessor(processorFactories, scriptService, type, normalizedScript);
        } else {
            throw newConfigurationException(type, null, null,
                "property isn't a map, but of type [" + config.getClass().getName() + "]");
        }
    }

    public static Processor readProcessor(Map<String, Processor.Factory> processorFactories,
                                           ScriptService scriptService,
                                           String type, Map<String, Object> config) throws Exception {
        String tag = ConfigurationUtils.readOptionalStringProperty(null, null, config, TAG_KEY);
        Script conditionalScript = extractConditional(config);
        Processor.Factory factory = processorFactories.get(type);
        if (factory != null) {
            boolean ignoreFailure = ConfigurationUtils.readBooleanProperty(null, null, config, "ignore_failure", false);
            List<Map<String, Object>> onFailureProcessorConfigs =
                ConfigurationUtils.readOptionalList(null, null, config, Pipeline.ON_FAILURE_KEY);

            List<Processor> onFailureProcessors = readProcessorConfigs(onFailureProcessorConfigs, scriptService, processorFactories);

            if (onFailureProcessorConfigs != null && onFailureProcessors.isEmpty()) {
                throw newConfigurationException(type, tag, Pipeline.ON_FAILURE_KEY,
                    "processors list cannot be empty");
            }

            try {
                Processor processor = factory.create(processorFactories, tag, config);
                if (config.isEmpty() == false) {
                    throw new ElasticsearchParseException("processor [{}] doesn't support one or more provided configuration parameters {}",
                        type, Arrays.toString(config.keySet().toArray()));
                }
                if (onFailureProcessors.size() > 0 || ignoreFailure) {
                    processor = new CompoundProcessor(ignoreFailure, Collections.singletonList(processor), onFailureProcessors);
                }
                if (conditionalScript != null) {
                    processor = new ConditionalProcessor(tag, conditionalScript, scriptService, processor);
                }
                return processor;
            } catch (Exception e) {
                throw newConfigurationException(type, tag, null, e);
            }
        }
        throw newConfigurationException(type, tag, null, "No processor type exists with name [" + type + "]");
    }

    private static Script extractConditional(Map<String, Object> config) throws IOException {
        Object scriptSource = config.remove("if");
        if (scriptSource != null) {
            try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)
                .map(normalizeScript(scriptSource));
                 InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
                return Script.parse(parser);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeScript(Object scriptConfig) {
        if (scriptConfig instanceof Map<?, ?>) {
            return (Map<String, Object>) scriptConfig;
        } else if (scriptConfig instanceof String) {
            return Collections.singletonMap("source", scriptConfig);
        } else {
            throw newConfigurationException("conditional", null, "script",
                "property isn't a map or string, but of type [" + scriptConfig.getClass().getName() + "]");
        }
    }
}
