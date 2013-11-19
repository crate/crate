package org.cratedb;

import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.Streams;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Properties;

public class Build {

    public static final Build CURRENT;

    static {
        String hash = "NA";
        String hashShort = "NA";
        String timestamp = "NA";

        try {
            String properties = Streams.copyToStringFromClasspath("/crate-build.properties");
            Properties props = new Properties();
            props.load(new FastStringReader(properties));
            hash = props.getProperty("hash", hash);
            if (!hash.equals("NA")) {
                hashShort = hash.substring(0, 7);
            }
            String gitTimestampRaw = props.getProperty("timestamp");
            if (gitTimestampRaw != null) {
                timestamp = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC).print(Long.parseLong(gitTimestampRaw));
            }
        } catch (Exception e) {
            // just ignore...
        }

        CURRENT = new Build(hash, hashShort, timestamp);
    }

    private String hash;
    private String hashShort;
    private String timestamp;

    Build(String hash, String hashShort, String timestamp) {
        this.hash = hash;
        this.hashShort = hashShort;
        this.timestamp = timestamp;
    }

    public String hash() {
        return hash;
    }

    public String hashShort() {
        return hashShort;
    }

    public String timestamp() {
        return timestamp;
    }
}
