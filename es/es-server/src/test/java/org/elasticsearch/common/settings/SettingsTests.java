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

package org.elasticsearch.common.settings;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SettingsTests extends ESTestCase {

    @Test
    public void testReplacePropertiesPlaceholderSystemProperty() {
        String value = System.getProperty("java.home");
        assertFalse(value.isEmpty());
        Settings settings = Settings.builder()
            .put("property.placeholder", value)
            .put("setting1", "${property.placeholder}")
            .replacePropertyPlaceholders()
            .build();
        assertThat(settings.get("setting1"), equalTo(value));
    }

    @Test
    public void testReplacePropertiesPlaceholderSystemPropertyList() {
        final String hostname = randomAlphaOfLength(16);
        final String hostip = randomAlphaOfLength(16);
        final Settings settings = Settings.builder()
            .putList("setting1", "${HOSTNAME}", "${HOSTIP}")
            .replacePropertyPlaceholders(name -> name.equals("HOSTNAME") ? hostname : name.equals("HOSTIP") ? hostip : null)
            .build();
        assertThat(settings.getAsList("setting1"), contains(hostname, hostip));
    }

    @Test
    public void testReplacePropertiesPlaceholderSystemVariablesHaveNoEffect() {
        final String value = System.getProperty("java.home");
        assertNotNull(value);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Settings.builder()
            .put("setting1", "${java.home}")
            .replacePropertyPlaceholders()
            .build());
        assertThat(e, hasToString(containsString("Could not resolve placeholder 'java.home'")));
    }

    @Test
    public void testReplacePropertiesPlaceholderByEnvironmentVariables() {
        final String hostname = randomAlphaOfLength(16);
        final Settings implicitEnvSettings = Settings.builder()
            .put("setting1", "${HOSTNAME}")
            .replacePropertyPlaceholders(name -> "HOSTNAME".equals(name) ? hostname : null)
            .build();
        assertThat(implicitEnvSettings.get("setting1"), equalTo(hostname));
    }

    @Test
    public void testGetAsSettings() {
        Settings settings = Settings.builder()
            .put("bar", "hello world")
            .put("foo", "abc")
            .put("foo.bar", "def")
            .put("foo.baz", "ghi").build();

        Settings fooSettings = settings.getAsSettings("foo");
        assertFalse(fooSettings.isEmpty());
        assertEquals(2, fooSettings.size());
        assertThat(fooSettings.get("bar"), equalTo("def"));
        assertThat(fooSettings.get("baz"), equalTo("ghi"));
    }

    @Test
    public void testMultLevelGetPrefix() {
        Settings settings = Settings.builder()
            .put("1.2.3", "hello world")
            .put("1.2.3.4", "abc")
            .put("2.3.4", "def")
            .put("3.4", "ghi").build();

        Settings firstLevelSettings = settings.getByPrefix("1.");
        assertFalse(firstLevelSettings.isEmpty());
        assertEquals(2, firstLevelSettings.size());
        assertThat(firstLevelSettings.get("2.3.4"), equalTo("abc"));
        assertThat(firstLevelSettings.get("2.3"), equalTo("hello world"));

        Settings secondLevelSetting = firstLevelSettings.getByPrefix("2.");
        assertFalse(secondLevelSetting.isEmpty());
        assertEquals(2, secondLevelSetting.size());
        assertNull(secondLevelSetting.get("2.3.4"));
        assertNull(secondLevelSetting.get("1.2.3.4"));
        assertNull(secondLevelSetting.get("1.2.3"));
        assertThat(secondLevelSetting.get("3.4"), equalTo("abc"));
        assertThat(secondLevelSetting.get("3"), equalTo("hello world"));

        Settings thirdLevelSetting = secondLevelSetting.getByPrefix("3.");
        assertFalse(thirdLevelSetting.isEmpty());
        assertEquals(1, thirdLevelSetting.size());
        assertNull(thirdLevelSetting.get("2.3.4"));
        assertNull(thirdLevelSetting.get("3.4"));
        assertNull(thirdLevelSetting.get("1.2.3"));
        assertThat(thirdLevelSetting.get("4"), equalTo("abc"));
    }

    @Test
    public void testNames() {
        Settings settings = Settings.builder()
            .put("bar", "baz")
            .put("foo", "abc")
            .put("foo.bar", "def")
            .put("foo.baz", "ghi").build();

        Set<String> names = settings.names();
        assertThat(names.size(), equalTo(2));
        assertTrue(names.contains("bar"));
        assertTrue(names.contains("foo"));

        Settings fooSettings = settings.getAsSettings("foo");
        names = fooSettings.names();
        assertThat(names.size(), equalTo(2));
        assertTrue(names.contains("bar"));
        assertTrue(names.contains("baz"));
    }

    @Test
    public void testThatArraysAreOverriddenCorrectly() throws IOException {
        // overriding a single value with an array
        Settings settings = Settings.builder()
            .put(Settings.builder().putList("value", "1").build())
            .put(Settings.builder().putList("value", "2", "3").build())
            .build();
        assertThat(settings.getAsList("value"), contains("2", "3"));

        settings = Settings.builder()
            .put(Settings.builder().put("value", "1").build())
            .put(Settings.builder().putList("value", "2", "3").build())
            .build();
        assertThat(settings.getAsList("value"), contains("2", "3"));
        settings = Settings.builder().loadFromSource("value: 1", XContentType.YAML)
            .loadFromSource("value: [ 2, 3 ]", XContentType.YAML)
            .build();
        assertThat(settings.getAsList("value"), contains("2", "3"));

        settings = Settings.builder()
            .put(Settings.builder().put("value.with.deep.key", "1").build())
            .put(Settings.builder().putList("value.with.deep.key", "2", "3").build())
            .build();
        assertThat(settings.getAsList("value.with.deep.key"), contains("2", "3"));

        // overriding an array with a shorter array
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2").build())
            .put(Settings.builder().putList("value", "3").build())
            .build();
        assertThat(settings.getAsList("value"), contains("3"));

        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2", "3").build())
            .put(Settings.builder().putList("value", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value"), contains("4", "5"));

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2", "3").build())
            .put(Settings.builder().putList("value.deep.key", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value.deep.key"), contains("4", "5"));

        // overriding an array with a longer array
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2").build())
            .put(Settings.builder().putList("value", "3", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value"), contains("3", "4", "5"));

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2", "3").build())
            .put(Settings.builder().putList("value.deep.key", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value.deep.key"), contains("4", "5"));

        // overriding an array with a single value
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2").build())
            .put(Settings.builder().put("value", "3").build())
            .build();
        assertThat(settings.getAsList("value"), contains("3"));

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2").build())
            .put(Settings.builder().put("value.deep.key", "3").build())
            .build();
        assertThat(settings.getAsList("value.deep.key"), contains("3"));

        // test that other arrays are not overridden
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "1", "2", "3").putList("a", "b", "c").build())
            .put(Settings.builder().putList("value", "4", "5").putList("d", "e", "f").build())
            .build();
        assertThat(settings.getAsList("value"), contains("4", "5"));
        assertThat(settings.getAsList("a"), contains("b", "c"));
        assertThat(settings.getAsList("d"), contains("e", "f"));

        settings = Settings.builder()
            .put(Settings.builder().putList("value.deep.key", "1", "2", "3").putList("a", "b", "c").build())
            .put(Settings.builder().putList("value.deep.key", "4", "5").putList("d", "e", "f").build())
            .build();
        assertThat(settings.getAsList("value.deep.key"), contains("4", "5"));
        assertThat(settings.getAsList("a"), notNullValue());
        assertThat(settings.getAsList("d"), notNullValue());

        // overriding a deeper structure with an array
        settings = Settings.builder()
            .put(Settings.builder().put("value.data", "1").build())
            .put(Settings.builder().putList("value", "4", "5").build())
            .build();
        assertThat(settings.getAsList("value"), contains("4", "5"));

        // overriding an array with a deeper structure
        settings = Settings.builder()
            .put(Settings.builder().putList("value", "4", "5").build())
            .put(Settings.builder().put("value.data", "1").build())
            .build();
        assertThat(settings.get("value.data"), is("1"));
        assertThat(settings.get("value"), is("[4, 5]"));
    }

    @Test
    public void testPrefixNormalization() {
        Settings settings = Settings.builder().normalizePrefix("foo.").build();

        assertThat(settings.names().size(), equalTo(0));

        settings = Settings.builder()
            .put("bar", "baz")
            .normalizePrefix("foo.")
            .build();

        assertThat(settings.size(), equalTo(1));
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar"), equalTo("baz"));


        settings = Settings.builder()
            .put("bar", "baz")
            .put("foo.test", "test")
            .normalizePrefix("foo.")
            .build();

        assertThat(settings.size(), equalTo(2));
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar"), equalTo("baz"));
        assertThat(settings.get("foo.test"), equalTo("test"));

        settings = Settings.builder()
            .put("foo.test", "test")
            .normalizePrefix("foo.")
            .build();


        assertThat(settings.size(), equalTo(1));
        assertThat(settings.get("foo.test"), equalTo("test"));
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
            assertTrue(k.startsWith("a.b"));
        }

        assertEquals(3, numKeys);
        assertFalse(filteredSettings.keySet().contains("a.c"));
        assertFalse(filteredSettings.keySet().contains("a"));
        assertTrue(filteredSettings.keySet().contains("a.b"));
        assertTrue(filteredSettings.keySet().contains("a.b.c"));
        assertTrue(filteredSettings.keySet().contains("a.b.c.d"));
        expectThrows(UnsupportedOperationException.class, () ->
            filteredSettings.keySet().remove("a.b"));
        assertEquals("ab1", filteredSettings.get("a.b"));
        assertEquals("ab2", filteredSettings.get("a.b.c"));
        assertEquals("ab3", filteredSettings.get("a.b.c.d"));

        Iterator<String> iterator = filteredSettings.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertTrue(iterator.hasNext());
        }
        assertEquals("a.b", iterator.next());
        if (randomBoolean()) {
            assertTrue(iterator.hasNext());
        }
        assertEquals("a.b.c", iterator.next());
        if (randomBoolean()) {
            assertTrue(iterator.hasNext());
        }
        assertEquals("a.b.c.d", iterator.next());
        assertFalse(iterator.hasNext());
        expectThrows(NoSuchElementException.class, () -> iterator.next());

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
            assertTrue(k, k.startsWith("b") || k.startsWith("c"));
        }

        assertEquals(4, numKeys);

        assertFalse(prefixMap.keySet().contains("a"));
        assertTrue(prefixMap.keySet().contains("c"));
        assertTrue(prefixMap.keySet().contains("b"));
        assertTrue(prefixMap.keySet().contains("b.c"));
        assertTrue(prefixMap.keySet().contains("b.c.d"));
        expectThrows(UnsupportedOperationException.class, () ->
            prefixMap.keySet().remove("a.b"));
        assertEquals("ab1", prefixMap.get("b"));
        assertEquals("ab2", prefixMap.get("b.c"));
        assertEquals("ab3", prefixMap.get("b.c.d"));
        Iterator<String> prefixIterator = prefixMap.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("b", prefixIterator.next());
        if (randomBoolean()) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("b.c", prefixIterator.next());
        if (randomBoolean()) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("b.c.d", prefixIterator.next());
        if (randomBoolean()) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("c", prefixIterator.next());
        assertFalse(prefixIterator.hasNext());
        expectThrows(NoSuchElementException.class, () -> prefixIterator.next());
    }

    @Test
    public void testSecureSettingsPrefix() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("test.prefix.foo", "somethingsecure");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        Settings prefixSettings = settings.getByPrefix("test.prefix.");
        assertTrue(prefixSettings.names().contains("foo"));
    }

    @Test
    public void testGroupPrefix() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("test.key1.foo", "somethingsecure");
        secureSettings.setString("test.key1.bar", "somethingsecure");
        secureSettings.setString("test.key2.foo", "somethingsecure");
        secureSettings.setString("test.key2.bog", "somethingsecure");
        Settings.Builder builder = Settings.builder();
        builder.put("test.key1.baz", "blah1");
        builder.put("test.key1.other", "blah2");
        builder.put("test.key2.baz", "blah3");
        builder.put("test.key2.else", "blah4");
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        Map<String, Settings> groups = settings.getGroups("test");
        assertEquals(2, groups.size());
        Settings key1 = groups.get("key1");
        assertNotNull(key1);
        assertThat(key1.names(), containsInAnyOrder("foo", "bar", "baz", "other"));
        Settings key2 = groups.get("key2");
        assertNotNull(key2);
        assertThat(key2.names(), containsInAnyOrder("foo", "bog", "baz", "else"));
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

        assertFalse(filteredSettings.keySet().contains("a.c"));
        assertFalse(filteredSettings.keySet().contains("a"));
        assertFalse(filteredSettings.keySet().contains("a.b"));
        assertFalse(filteredSettings.keySet().contains("a.b.c"));
        assertFalse(filteredSettings.keySet().contains("a.b.c.d"));
        expectThrows(UnsupportedOperationException.class, () ->
            filteredSettings.keySet().remove("a.b"));
        assertNull(filteredSettings.get("a.b"));
        assertNull(filteredSettings.get("a.b.c"));
        assertNull(filteredSettings.get("a.b.c.d"));

        Iterator<String> iterator = filteredSettings.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertFalse(iterator.hasNext());
        }
        expectThrows(NoSuchElementException.class, () -> iterator.next());
    }

    @Test
    public void testEmpty() {
        assertTrue(Settings.EMPTY.isEmpty());
        MockSecureSettings secureSettings = new MockSecureSettings();
        assertTrue(Settings.builder().setSecureSettings(secureSettings).build().isEmpty());
    }

    @Test
    public void testWriteSettingsToStream() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("test.key1.foo", "somethingsecure");
        secureSettings.setString("test.key1.bar", "somethingsecure");
        secureSettings.setString("test.key2.foo", "somethingsecure");
        secureSettings.setString("test.key2.bog", "somethingsecure");
        Settings.Builder builder = Settings.builder();
        builder.put("test.key1.baz", "blah1");
        builder.putNull("test.key3.bar");
        builder.putList("test.key4.foo", "1", "2");
        builder.setSecureSettings(secureSettings);
        assertEquals(7, builder.build().size());
        Settings.writeSettingsToStream(builder.build(), out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        Settings settings = Settings.readSettingsFromStream(in);
        assertEquals(3, settings.size());
        assertEquals("blah1", settings.get("test.key1.baz"));
        assertNull(settings.get("test.key3.bar"));
        assertTrue(settings.keySet().contains("test.key3.bar"));
        assertEquals(Arrays.asList("1", "2"), settings.getAsList("test.key4.foo"));
    }

    @Test
    public void testGetAsArrayFailsOnDuplicates() {
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> Settings.builder()
            .put("foobar.0", "bar")
            .put("foobar.1", "baz")
            .put("foobar", "foo")
            .build());
        assertThat(e,
                   hasToString(containsString(
                       "settings builder can't contain values for [foobar=foo] and [foobar.0=bar]")));
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

        assertThat(settings.get("test1.value1"), equalTo("value1"));
        assertThat(settings.get("test1.test2.value2"), equalTo("value2"));
        assertThat(settings.getAsInt("test1.test2.value3", -1), equalTo(2));

        // check array
        assertNull(settings.get("test1.test3.0"));
        assertNull(settings.get("test1.test3.1"));
        assertThat(settings.getAsList("test1.test3").size(), equalTo(2));
        assertThat(settings.getAsList("test1.test3").get(0), equalTo("test3-1"));
        assertThat(settings.getAsList("test1.test3").get(1), equalTo("test3-2"));
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
        Settings.writeSettingsToStream(settings, output);
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
        assertTrue(Settings.builder().copy("test", settings).build().keySet().contains("test"));
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                                                    () -> Settings.builder().copy("not_there", settings));
        assertEquals("source key not found in the source settings", iae.getMessage());
    }
}
