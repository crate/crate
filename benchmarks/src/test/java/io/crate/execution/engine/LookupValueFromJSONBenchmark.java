package io.crate.execution.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(value = 1)
@State(Scope.Benchmark)
public class LookupValueFromJSONBenchmark {

    private List<String> keysToLookup;
    private String jsonString;

    @Param({"10", "500", "1000"})
    public int numKeys;

    @Param({"1", "5", "10", "50", "500", "750", "1000"})
    public int numLookups;

    @Setup
    public void generate_json() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ArrayList<String> lookupCandidates = new ArrayList<>();
        StringBuilder json = new StringBuilder();
        json.append("{");
        for (int i = 0; i < numKeys; i++) {
            String key = UUIDs.base64UUID();
            lookupCandidates.add(key);
            json.append("\"" + key + "\": ");
            json.append(random.nextInt());
            if (i + 1 < numKeys) {
                json.append(",");
            }
        }
        json.append("}");
        jsonString = json.toString();

        keysToLookup = new ArrayList<String>();
        for (int i = 0; i < numLookups; i++) {
            keysToLookup.add(lookupCandidates.get(random.nextInt(0, lookupCandidates.size())));
        }
    }

    @Benchmark
    public void parse_json_and_retrieve_field_from_it(Blackhole blackhole) throws IOException {
        for (int i = 0; i < keysToLookup.size(); i++) {
            String key = keysToLookup.get(i);
            XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                jsonString
            );
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentName = parser.currentName();
                    parser.nextToken();
                    if (key.equals(currentName)) {
                        blackhole.consume(parser.text());
                        break;
                    }
                }
            }
        }
    }

    @Benchmark
    public void parse_document_into_filtered_map_and_retrieve_fields_from_it(Blackhole blackhole) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            jsonString
        );
        HashMap<String, Object> map = new HashMap<>();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        HashSet<String> includes = new HashSet<>(keysToLookup);
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            token = parser.nextToken();
            Object value = parser.text();
            if (includes.contains(fieldName)) {
                map.put(fieldName, value);
            }
        }
        for (int i = 0; i < keysToLookup.size(); i++) {
            blackhole.consume(map.get(keysToLookup.get(i)));
        }
    }

    @Benchmark
    public void parse_document_into_map_and_retrieve_field_from_it(Blackhole blackhole) {
        BytesArray contents = new BytesArray(jsonString);
        Map<String, Object> map = XContentHelper.toMap(contents, XContentType.JSON);
        for (int i = 0; i < keysToLookup.size(); i++) {
            blackhole.consume(map.get(keysToLookup.get(i)));
        }
    }
}
