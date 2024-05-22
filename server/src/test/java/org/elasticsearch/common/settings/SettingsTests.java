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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

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
        assertNotNull(value);
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
        assertEquals(2, fooSettings.size());
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
        assertEquals(2, firstLevelSettings.size());
        assertThat(firstLevelSettings.get("2.3.4")).isEqualTo("abc");
        assertThat(firstLevelSettings.get("2.3")).isEqualTo("hello world");

        Settings secondLevelSetting = firstLevelSettings.getByPrefix("2.");
        assertThat(secondLevelSetting.isEmpty()).isFalse();
        assertEquals(2, secondLevelSetting.size());
        assertNull(secondLevelSetting.get("2.3.4"));
        assertNull(secondLevelSetting.get("1.2.3.4"));
        assertNull(secondLevelSetting.get("1.2.3"));
        assertThat(secondLevelSetting.get("3.4")).isEqualTo("abc");
        assertThat(secondLevelSetting.get("3")).isEqualTo("hello world");

        Settings thirdLevelSetting = secondLevelSetting.getByPrefix("3.");
        assertThat(thirdLevelSetting.isEmpty()).isFalse();
        assertEquals(1, thirdLevelSetting.size());
        assertNull(thirdLevelSetting.get("2.3.4"));
        assertNull(thirdLevelSetting.get("3.4"));
        assertNull(thirdLevelSetting.get("1.2.3"));
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
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar")).isEqualTo("baz");


        settings = Settings.builder()
            .put("bar", "baz")
            .put("foo.test", "test")
            .normalizePrefix("foo.")
            .build();

        assertThat(settings).hasSize(2);
        assertThat(settings.get("bar"), nullValue());
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


        Settings filteredSettings = builder.build().filter((k) -> k.startsWith("a.b"));
        assertEquals(3, filteredSettings.size());
        int numKeys = 0;
        for (String k : filteredSettings.keySet()) {
            numKeys++;
            assertThat(k.startsWith("a.b")).isTrue();
        }

        assertEquals(3, numKeys);
        assertThat(filteredSettings.keySet().contains("a.c")).isFalse();
        assertThat(filteredSettings.keySet().contains("a")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b")).isTrue();
        assertThat(filteredSettings.keySet().contains("a.b.c")).isTrue();
        assertThat(filteredSettings.keySet().contains("a.b.c.d")).isTrue();
        assertThatThrownBy(() -> filteredSettings.keySet().remove("a.b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertEquals("ab1", filteredSettings.get("a.b"));
        assertEquals("ab2", filteredSettings.get("a.b.c"));
        assertEquals("ab3", filteredSettings.get("a.b.c.d"));

        Iterator<String> iterator = filteredSettings.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertThat(iterator.hasNext()).isTrue();
        }
        assertEquals("a.b", iterator.next());
        if (randomBoolean()) {
            assertThat(iterator.hasNext()).isTrue();
        }
        assertEquals("a.b.c", iterator.next());
        if (randomBoolean()) {
            assertThat(iterator.hasNext()).isTrue();
        }
        assertEquals("a.b.c.d", iterator.next());
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
        assertEquals(4, prefixMap.size());
        int numKeys = 0;
        for (String k : prefixMap.keySet()) {
            numKeys++;
            assertThat(k.startsWith("b") || k.startsWith("c")).as(k).isTrue();
        }

        assertEquals(4, numKeys);

        assertThat(prefixMap.keySet().contains("a")).isFalse();
        assertThat(prefixMap.keySet().contains("c")).isTrue();
        assertThat(prefixMap.keySet().contains("b")).isTrue();
        assertThat(prefixMap.keySet().contains("b.c")).isTrue();
        assertThat(prefixMap.keySet().contains("b.c.d")).isTrue();
        assertThatThrownBy(() -> prefixMap.keySet().remove("a.b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertEquals("ab1", prefixMap.get("b"));
        assertEquals("ab2", prefixMap.get("b.c"));
        assertEquals("ab3", prefixMap.get("b.c.d"));
        Iterator<String> prefixIterator = prefixMap.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertEquals("b", prefixIterator.next());
        if (randomBoolean()) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertEquals("b.c", prefixIterator.next());
        if (randomBoolean()) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertEquals("b.c.d", prefixIterator.next());
        if (randomBoolean()) {
            assertThat(prefixIterator.hasNext()).isTrue();
        }
        assertEquals("c", prefixIterator.next());
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
        assertEquals(2, groups.size());
        Settings key1 = groups.get("key1");
        assertNotNull(key1);
        assertThat(key1.names(), containsInAnyOrder("baz", "other"));
        Settings key2 = groups.get("key2");
        assertNotNull(key2);
        assertThat(key2.names(), containsInAnyOrder("baz", "else"));
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
        assertEquals(0, filteredSettings.size());

        assertThat(filteredSettings.keySet().contains("a.c")).isFalse();
        assertThat(filteredSettings.keySet().contains("a")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b.c")).isFalse();
        assertThat(filteredSettings.keySet().contains("a.b.c.d")).isFalse();
        assertThatThrownBy(() -> filteredSettings.keySet().remove("a.b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertNull(filteredSettings.get("a.b"));
        assertNull(filteredSettings.get("a.b.c"));
        assertNull(filteredSettings.get("a.b.c.d"));

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
        assertEquals(3, builder.build().size());
        Settings.writeSettingsToStream(out, builder.build());
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        Settings settings = Settings.readSettingsFromStream(in);
        assertEquals(3, settings.size());
        assertEquals("blah1", settings.get("test.key1.baz"));
        assertNull(settings.get("test.key3.bar"));
        assertThat(settings.keySet().contains("test.key3.bar")).isTrue();
        assertEquals(Arrays.asList("1", "2"), settings.getAsList("test.key4.foo"));
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
        assertEquals(5, build.size());
        assertEquals(Arrays.asList("1", "2", "3"), build.getAsList("foo.bar.baz"));
        assertEquals(2, build.getAsInt("foo.foobar", 0).intValue());
        assertEquals("test", build.get("rootfoo"));
        assertEquals("1,2,3,4", build.get("foo.baz"));
        assertNull(build.get("foo.null.baz"));
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
        assertNull(settings.get("test1.test3.0"));
        assertNull(settings.get("test1.test3.1"));
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
        assertEquals("{\"foo\":{\"bar.baz\":\"test\",\"bar\":[\"1\",\"2\",\"3\"]}}", Strings.toString(builder));

        test = Settings.builder().putList("foo.bar", "1", "2", "3").build();
        builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        test.toXContent(builder, new ToXContent.MapParams(Collections.emptyMap()));
        builder.endObject();
        assertEquals("{\"foo\":{\"bar\":[\"1\",\"2\",\"3\"]}}", Strings.toString(builder));

        builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        test.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject();
        assertEquals("{\"foo.bar\":[\"1\",\"2\",\"3\"]}", Strings.toString(builder));
    }

    @Test
    public void testLoadEmptyStream() throws IOException {
        Settings test = Settings.builder().loadFromStream("test.json",
                                                          new ByteArrayInputStream(new byte[0]),
                                                          false)
            .build();
        assertEquals(0, test.size());
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
        assertEquals(2, build.size());
        assertEquals(build.getAsList("foo.bar"), Arrays.asList("0", "1", "2", "3"));
        assertEquals(build.get("foo.bar.baz"), "baz");
    }

    @Test
    public void testCopy() {
        Settings settings = Settings.builder().putList("foo.bar", "0", "1", "2", "3").put("foo.bar.baz", "baz").putNull(
            "test").build();
        assertEquals(Arrays.asList("0", "1", "2", "3"),
                     Settings.builder().copy("foo.bar", settings).build().getAsList("foo.bar"));
        assertEquals("baz", Settings.builder().copy("foo.bar.baz", settings).build().get("foo.bar.baz"));
        assertNull(Settings.builder().copy("foo.bar.baz", settings).build().get("test"));
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
        assertThat(map, hasEntry("masked", "masked_value"));
    }

    @Test
    public void testGetAsStructuredMapMaskingValues() {
        Settings settings = Settings.builder()
            .put("masked", "masked_value")
            .build();

        Map<String, Object> map = settings.getAsStructuredMap(Set.of("masked"));
        assertThat(map, hasEntry("masked", Settings.MASKED_VALUE));
    }
}
