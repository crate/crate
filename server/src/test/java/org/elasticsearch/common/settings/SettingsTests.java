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

package org.elasticsearch.common.settings;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class SettingsTests extends ESTestCase {

    @Test
    public void testReplacePropertiesPlaceholderSystemProperty() {
        String value = System.getProperty("java.home");
        assertThat(value.isEmpty()).isFalse();
        Settings settings = Settings.builder()
            .put("property.placeholder", value)
            .put("setting1", "${property.placeholder}")
            .replacePropertyPlaceholders()
            .build();
        assertThat(settings.get("setting1")).isEqualTo(value);
    }

    @Test
    public void testReplacePropertiesPlaceholderSystemPropertyList() {
        final String hostname = randomAlphaOfLength(16);
        final String hostip = randomAlphaOfLength(16);
        final Settings settings = Settings.builder()
            .putList("setting1", "${HOSTNAME}", "${HOSTIP}")
            .replacePropertyPlaceholders(name -> name.equals("HOSTNAME") ? hostname : name.equals("HOSTIP") ? hostip : null)
            .build();
        assertThat(settings.getAsList("setting1")).containsExactly(hostname, hostip);
    }

    @Test
    public void testReplacePropertiesPlaceholderSystemVariablesHaveNoEffect() {
        final String value = System.getProperty("java.home");
        assertThat(value).isNotNull();
        assertThatThrownBy(() -> Settings.builder()
            .put("setting1", "${java.home}")
            .replacePropertyPlaceholders()
            .build()
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Could not resolve placeholder 'java.home'");
    }

    @Test
    public void testReplacePropertiesPlaceholderByEnvironmentVariables() {
        final String hostname = randomAlphaOfLength(16);
        final Settings implicitEnvSettings = Settings.builder()
            .put("setting1", "${HOSTNAME}")
            .replacePropertyPlaceholders(name -> "HOSTNAME".equals(name) ? hostname : null)
            .build();
        assertThat(implicitEnvSettings.get("setting1")).isEqualTo(hostname);
    }

    @Test
    public void testGetAsSettings() {
        Settings settings = Settings.builder()
            .put("bar", "hello world")
            .put("foo", "abc")
            .put("foo.bar", "def")
            .put("foo.baz", "ghi").build();

        Settings fooSettings = settings.getAsSettings("foo");
        assertThat(fooSettings.isEmpty()).isFalse();
        assertThat(fooSettings.size()).isEqualTo(2);
        assertThat(fooSettings.get("bar")).isEqualTo("def");
        assertThat(fooSettings.get("baz")).isEqualTo("ghi");
    }

    @Test
    public void testMultLevelGetPrefix() {
        Settings settings = Settings.builder()
            .put("1.2.3", "hello world")
            .put("1.2.3.4", "abc")
            .put("2.3.4", "def")
            .put("3.4", "ghi").build();

        Settings firstLevelSettings = settings.getByPrefix("1.");
        assertThat(firstLevelSettings.isEmpty()).isFalse();
        assertThat(firstLevelSettings.size()).isEqualTo(2);
        assertThat(firstLevelSettings.get("2.3.4")).isEqualTo("abc");
        assertThat(firstLevelSettings.get("2.3")).isEqualTo("hello world");

        Settings secondLevelSetting = firstLevelSettings.getByPrefix("2.");
        assertThat(secondLevelSetting.isEmpty()).isFalse();
        assertThat(secondLevelSetting.size()).isEqualTo(2);
        assertThat(secondLevelSetting.get("2.3.4")).isNull();
        assertThat(secondLevelSetting.get("1.2.3.4")).isNull();
        assertThat(secondLevelSetting.get("1.2.3")).isNull();
        assertThat(secondLevelSetting.get("3.4")).isEqualTo("abc");
        assertThat(secondLevelSetting.get("3")).isEqualTo("hello world");

        Settings thirdLevelSetting = secondLevelSetting.getByPrefix("3.");
        assertThat(thirdLevelSetting.isEmpty()).isFalse();
        assertThat(thirdLevelSetting.size()).isEqualTo(1);
        assertThat(thirdLevelSetting.get("2.3.4")).isNull();
        assertThat(thirdLevelSetting.get("3.4")).isNull();
        assertThat(thirdLevelSetting.get("1.2.3")).isNull();
        assertThat(thirdLevelSetting.get("4")).isEqualTo("abc");
    }

    @Test
    public void testNames() {
        Settings settings = Settings.builder()
            .put("bar", "baz")
            .put("foo", "abc")
            .put("foo.bar", "def")
            .put("foo.baz", "ghi").build();

        Set<String> names = settings.names();
        assertThat(names).hasSize(2);
        assertThat(names.contains("bar")).isTrue();
        assertThat(names.contains("foo")).isTrue();

        Settings fooSettings = settings.getAsSettings("foo");
        names = fooSettings.names();
        assertThat(names).hasSize(2);
        assertThat(names.contains("bar")).isTrue();
        assertThat(names.contains("baz")).isTrue();
    }

    @Test
    public void testThatArraysAreOverriddenCorrectly() throws IOException {
        // overriding a single value with an array
        Settings settings = Settings.builder()
            .put(Settings.builder().putList("value", "1").build())
            .put(Settings.builder().putList("value", "2", "3").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("2", "3");

        settings = Settings.builder()
            .put(Settings.builder().put("value", "1").build())
            .put(Settings.builder().putList("value", "2", "3").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("2", "3");
        settings = Settings.builder().loadFromSource("value: 1", XContentType.YAML)
            .loadFromSource("value: [ 2, 3 ]", XContentType.YAML)
            .build();
        assertThat(settings.getAsList("value")).containsExactly("2", "3");

        settings = Settings.builder()
            .put(Settings.builder().put("value.with.deep.key", "1").build())
            .put(Settings.builder().putList("value.with.deep.key", "2", "3").build())
            .build();
        assertThat(settings.getAsList("value.with.deep.key")).containsExactly("2", "3");

        // overriding an array with a shorter array
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2").build())
            .put(Settings.builder().putList("value", "3").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("3");

        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2", "3").build())
            .put(Settings.builder().putList("value", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("4", "5");

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2", "3").build())
            .put(Settings.builder().putList("value.deep.key", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value.deep.key")).containsExactly("4", "5");

        // overriding an array with a longer array
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2").build())
            .put(Settings.builder().putList("value", "3", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("3", "4", "5");

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2", "3").build())
            .put(Settings.builder().putList("value.deep.key", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value.deep.key")).containsExactly("4", "5");

        // overriding an array with a single value
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2").build())
            .put(Settings.builder().put("value", "3").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("3");

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2").build())
            .put(Settings.builder().put("value.deep.key", "3").build())
            .build();
        assertThat(settings.getAsList("value.deep.key")).containsExactly("3");

        // test that other arrays are not overridden
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2", "3").putList("a", "b", "c").build())
            .put(Settings.builder().putList("value", "4", "5").putList("d", "e", "f").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("4", "5");
        assertThat(settings.getAsList("a")).containsExactly("b", "c");
        assertThat(settings.getAsList("d")).containsExactly("e", "f");

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2", "3").putList("a", "b", "c").build())
            .put(Settings.builder().putList("value.deep.key", "4", "5").putList("d", "e", "f").build())
            .build();
        assertThat(settings.getAsList("value.deep.key")).containsExactly("4", "5");
        assertThat(settings.getAsList("a")).isNotNull();
        assertThat(settings.getAsList("d")).isNotNull();

        // overriding a deeper structure with an array
        settings = Settings.builder()
            .put(Settings.builder().put("value.data", "1").build())
            .put(Settings.builder().putList("value", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value")).containsExactly("4", "5");

        // overriding an array with a deeper structure
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "4", "5").build())
            .put(Settings.builder().put("value.data", "1").build())
            .build();
        assertThat(settings.get("value.data")).isEqualTo("1");
        assertThat(settings.get("value")).isEqualTo("[4, 5]");
    }

    @Test
    public void testPrefixNormalization() {
        Settings settings = Settings.builder().normalizePrefix("foo.").build();

        assertThat(settings.names()).hasSize(0);

        settings = Settings.builder()
            .put("bar", "baz")
            .normalizePrefix("foo.")
            .build();

        assertThat(settings).hasSize(1);
        assertThat(settings.get("bar")).isNull();
        assertThat(settings.get("foo.bar")).isEqualTo("baz");


        settings = Settings.builder()
            .put("bar", "baz")
            .put("foo.test", "test")
            .normalizePrefix("foo.")
            .build();

        assertThat(settings).hasSize(2);
        assertThat(settings.get("bar")).isNull();
        assertThat(settings.get("foo.bar")).isEqualTo("baz");
        assertThat(settings.get("foo.test")).isEqualTo("test");

        settings = Settings.builder()
            .put("foo.test", "test")
            .normalizePrefix("foo.")
            .build();


        assertThat(settings).hasSize(1);
        assertThat(settings.get("foo.test")).isEqualTo("test");
    }

    @Test
    public void testFilteredMap() {
        Settings.Builder builder = Settings.builder();
        builder.put("a", "a1");
        builder.put("a.b", "ab1");
        builder.put("a.b.c", "ab2");
        builder.put("a.c", "ac1");
        builder.put("a.b.c.d", "ab3");


        Settings filteredSettings = builder.build().filter(k -> k.startsWith("a.b"));
        assertThat(filteredSettings.size()).isEqualTo(3);
        int numKeys = 0;
        for (String k : filteredSettings.keySet()) {
            numKeys++;
            assertThat(k.startsWith("a.b")).isTrue();
        }

        assertThat(numKeys).isEqualTo(3);
        assertThat(filteredSettings.keySet().contains("a.c")).isFalse();
        assertThat(filteredSettings.keySet().contains("a")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b")).isTrue();
        assertThat(filteredSettings.keySet().contains("a.b.c")).isTrue();
        assertThat(filteredSettings.keySet().contains("a.b.c.d")).isTrue();
        assertThatThrownBy(() -> filteredSettings.keySet().remove("a.b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertThat(filteredSettings.get("a.b")).isEqualTo("ab1");
        assertThat(filteredSettings.get("a.b.c")).isEqualTo("ab2");
        assertThat(filteredSettings.get("a.b.c.d")).isEqualTo("ab3");

        Iterator<String> iterator = filteredSettings.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertThat(iterator.hasNext()).isTrue();
        }
        assertThat(iterator.next()).isEqualTo("a.b");
        if (randomBoolean()) {
            assertThat(iterator.hasNext()).isTrue();
        }
        assertThat(iterator.next()).isEqualTo("a.b.c");
        if (randomBoolean()) {
            assertThat(iterator.hasNext()).isTrue();
        }
        assertThat(iterator.next()).isEqualTo("a.b.c.d");
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(() -> iterator.next())
            .isExactlyInstanceOf(NoSuchElementException.class);

    }

    @Test
    public void testPrefixMap() {
        Settings.Builder builder = Settings.builder();
        builder.put("a", "a1");
        builder.put("a.b", "ab1");
        builder.put("a.b.c", "ab2");
        builder.put("a.c", "ac1");
        builder.put("a.b.c.d", "ab3");

        Settings prefixMap = builder.build().getByPrefix("a.");
        assertThat(prefixMap.size()).isEqualTo(4);
        int numKeys = 0;
        for (String k : prefixMap.keySet()) {
            numKeys++;
            assertThat(k.startsWith("b") || k.startsWith("c")).as(k).isTrue();
        }

        assertThat(numKeys).isEqualTo(4);

        assertThat(prefixMap.keySet().contains("a")).isFalse();
        assertThat(prefixMap.keySet().contains("c")).isTrue();
        assertThat(prefixMap.keySet().contains("b")).isTrue();
        assertThat(prefixMap.keySet().contains("b.c")).isTrue();
        assertThat(prefixMap.keySet().contains("b.c.d")).isTrue();
        assertThatThrownBy(() -> prefixMap.keySet().remove("a.b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertThat(prefixMap.get("b")).isEqualTo("ab1");
        assertThat(prefixMap.get("b.c")).isEqualTo("ab2");
        assertThat(prefixMap.get("b.c.d")).isEqualTo("ab3");
        Iterator<String> prefixIterator = prefixMap.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertThat(prefixIterator.next()).isEqualTo("b");
        if (randomBoolean()) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertThat(prefixIterator.next()).isEqualTo("b.c");
        if (randomBoolean()) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertThat(prefixIterator.next()).isEqualTo("b.c.d");
        if (randomBoolean()) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertThat(prefixIterator.next()).isEqualTo("c");
        assertThat(prefixIterator.hasNext()).isFalse();
        assertThatThrownBy(() -> prefixIterator.next())
            .isExactlyInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testGroupPrefix() {
        Settings.Builder builder = Settings.builder();
        builder.put("test.key1.baz", "blah1");
        builder.put("test.key1.other", "blah2");
        builder.put("test.key2.baz", "blah3");
        builder.put("test.key2.else", "blah4");
        Settings settings = builder.build();
        Map<String, Settings> groups = settings.getGroups("test");
        assertThat(groups.size()).isEqualTo(2);
        Settings key1 = groups.get("key1");
        assertThat(key1).isNotNull();
        assertThat(key1.names()).containsExactlyInAnyOrder("baz", "other");
        Settings key2 = groups.get("key2");
        assertThat(key2).isNotNull();
        assertThat(key2.names()).containsExactlyInAnyOrder("baz", "else");
    }

    @Test
    public void testEmptyFilterMap() {
        Settings.Builder builder = Settings.builder();
        builder.put("a", "a1");
        builder.put("a.b", "ab1");
        builder.put("a.b.c", "ab2");
        builder.put("a.c", "ac1");
        builder.put("a.b.c.d", "ab3");

        Settings filteredSettings = builder.build().filter((k) -> false);
        assertThat(filteredSettings.size()).isEqualTo(0);

        assertThat(filteredSettings.keySet().contains("a.c")).isFalse();
        assertThat(filteredSettings.keySet().contains("a")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b.c")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b.c.d")).isFalse();
        assertThatThrownBy(() -> filteredSettings.keySet().remove("a.b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertThat(filteredSettings.get("a.b")).isNull();
        assertThat(filteredSettings.get("a.b.c")).isNull();
        assertThat(filteredSettings.get("a.b.c.d")).isNull();

        Iterator<String> iterator = filteredSettings.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertThat(iterator.hasNext()).isFalse();
        }
        assertThatThrownBy(() -> iterator.next())
            .isExactlyInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testEmpty() {
        assertThat(Settings.EMPTY.isEmpty()).isTrue();
    }

    @Test
    public void testWriteSettingsToStream() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        Settings.Builder builder = Settings.builder();
        builder.put("test.key1.baz", "blah1");
        builder.putNull("test.key3.bar");
        builder.putList("test.key4.foo", "1", "2");
        assertThat(builder.build().size()).isEqualTo(3);
        Settings.writeSettingsToStream(out, builder.build());
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        Settings settings = Settings.readSettingsFromStream(in);
        assertThat(settings.size()).isEqualTo(3);
        assertThat(settings.get("test.key1.baz")).isEqualTo("blah1");
        assertThat(settings.get("test.key3.bar")).isNull();
        assertThat(settings.keySet().contains("test.key3.bar")).isTrue();
        assertThat(settings.getAsList("test.key4.foo")).isEqualTo(Arrays.asList("1", "2"));
    }

    @Test
    public void testGetAsArrayFailsOnDuplicates() {
        assertThatThrownBy(() -> Settings.builder()
            .put("foobar.0", "bar")
            .put("foobar.1", "baz")
            .put("foobar", "foo")
            .build()
        ).isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("settings builder can't contain values for [foobar=foo] and [foobar.0=bar]");
    }

    @Test
    public void testToAndFromXContent() throws IOException {
        Settings settings = Settings.builder()
            .putList("foo.bar.baz", "1", "2", "3")
            .put("foo.foobar", 2)
            .put("rootfoo", "test")
            .put("foo.baz", "1,2,3,4")
            .putNull("foo.null.baz")
            .build();
        final boolean flatSettings = randomBoolean();
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        settings.toXContent(builder,
                            new ToXContent.MapParams(Collections.singletonMap("flat_settings", "" + flatSettings)));
        builder.endObject();
        XContentParser parser = createParser(builder);
        Settings build = Settings.fromXContent(parser);
        assertThat(build.size()).isEqualTo(5);
        assertThat(build.getAsList("foo.bar.baz")).isEqualTo(Arrays.asList("1", "2", "3"));
        assertThat(build.getAsInt("foo.foobar", 0).intValue()).isEqualTo(2);
        assertThat(build.get("rootfoo")).isEqualTo("test");
        assertThat(build.get("foo.baz")).isEqualTo("1,2,3,4");
        assertThat(build.get("foo.null.baz")).isNull();
    }

    @Test
    public void testSimpleJsonSettings() throws Exception {
        final String json = "/org/elasticsearch/common/settings/loader/test-settings.json";
        final Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .build();

        assertThat(settings.get("test1.value1")).isEqualTo("value1");
        assertThat(settings.get("test1.test2.value2")).isEqualTo("value2");
        assertThat(settings.getAsInt("test1.test2.value3", -1)).isEqualTo(2);

        // check array
        assertThat(settings.get("test1.test3.0")).isNull();
        assertThat(settings.get("test1.test3.1")).isNull();
        assertThat(settings.getAsList("test1.test3")).hasSize(2);
        assertThat(settings.getAsList("test1.test3").get(0)).isEqualTo("test3-1");
        assertThat(settings.getAsList("test1.test3").get(1)).isEqualTo("test3-2");
    }

    @Test
    public void testToXContent() throws IOException {
        // this is just terrible but it's the existing behavior!
        Settings test = Settings.builder().putList("foo.bar", "1", "2", "3").put("foo.bar.baz", "test").build();
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        test.toXContent(builder, new ToXContent.MapParams(Collections.emptyMap()));
        builder.endObject();
        assertThat(Strings.toString(builder)).isEqualTo("{\"foo\":{\"bar.baz\":\"test\",\"bar\":[\"1\",\"2\",\"3\"]}}");

        test = Settings.builder().putList("foo.bar", "1", "2", "3").build();
        builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        test.toXContent(builder, new ToXContent.MapParams(Collections.emptyMap()));
        builder.endObject();
        assertThat(Strings.toString(builder)).isEqualTo("{\"foo\":{\"bar\":[\"1\",\"2\",\"3\"]}}");

        builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        test.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject();
        assertThat(Strings.toString(builder)).isEqualTo("{\"foo.bar\":[\"1\",\"2\",\"3\"]}");
    }

    @Test
    public void testLoadEmptyStream() throws IOException {
        Settings test = Settings.builder().loadFromStream("test.json",
                                                          new ByteArrayInputStream(new byte[0]),
                                                          false)
            .build();
        assertThat(test.size()).isEqualTo(0);
    }

    @Test
    public void testReadWriteArray() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        //output.setVersion(randomFrom(Version.CURRENT, Version.V_7_0_0));
        output.setVersion(Version.CURRENT);
        Settings settings = Settings.builder().putList("foo.bar", "0", "1", "2", "3").put("foo.bar.baz", "baz").build();
        Settings.writeSettingsToStream(output, settings);
        StreamInput in = StreamInput.wrap(BytesReference.toBytes(output.bytes()));
        Settings build = Settings.readSettingsFromStream(in);
        assertThat(build.size()).isEqualTo(2);
        assertThat(build.getAsList("foo.bar")).isEqualTo(Arrays.asList("0", "1", "2", "3"));
        assertThat("baz").isEqualTo(build.get("foo.bar.baz"));
    }

    @Test
    public void testCopy() {
        Settings settings = Settings.builder().putList("foo.bar", "0", "1", "2", "3").put("foo.bar.baz", "baz").putNull(
            "test").build();
        assertThat(Settings.builder().copy("foo.bar", settings).build().getAsList("foo.bar")).isEqualTo(Arrays.asList("0", "1", "2", "3"));
        assertThat(Settings.builder().copy("foo.bar.baz", settings).build().get("foo.bar.baz")).isEqualTo("baz");
        assertThat(Settings.builder().copy("foo.bar.baz", settings).build().get("test")).isNull();
        assertThat(Settings.builder().copy("test", settings).build().keySet().contains("test")).isTrue();
        assertThatThrownBy(() -> Settings.builder().copy("not_there", settings))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("source key not found in the source settings");
    }

    @Test
    public void testGetAsStructuredMapNoSettingToMask() {
        Settings settings = Settings.builder()
            .put("masked", "masked_value")
            .build();

        Map<String, Object> map = settings.getAsStructuredMap(Set.of());
        assertThat(map).containsEntry("masked", "masked_value");
    }

    @Test
    public void testGetAsStructuredMapMaskingValues() {
        Settings settings = Settings.builder()
            .put("masked", "masked_value")
            .build();

        Map<String, Object> map = settings.getAsStructuredMap(Set.of("masked"));
        assertThat(map).containsEntry("masked", Settings.MASKED_VALUE);
    }
}
