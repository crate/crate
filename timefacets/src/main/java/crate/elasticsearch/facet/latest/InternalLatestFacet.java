package crate.elasticsearch.facet.latest;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.List;

/**
 * Collapses matching documents to ``key_field`` and uses only
 * the document with the highest value of ``ts_field``.
  */
public class InternalLatestFacet extends InternalFacet {

    public static final String TYPE = "latest";

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    protected String name;
    protected int size;
    protected int start;
    protected int total = 0;

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    public Facet reduce(List<Facet> facets) {
        InternalLatestFacet first = (InternalLatestFacet) facets.get(0);
        return first.reduce(name, facets);
    }

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readDistinctTermsFacet(in);
        }
    };

    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return type();
    }

    public InternalLatestFacet() {
        super();
    }

    public InternalLatestFacet(String facetName, int size, int start, int total) {
        super(facetName);
        this.size = size;
        this.start = start;
        this.name = facetName;
        this.total = total;
    }


    public EntryPriorityQueue queue;

    public static class EntryPriorityQueue extends PriorityQueue<Entry> {

        public EntryPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Entry a, Entry b) {
            return a.value < b.value;
        }
    }

    public void insert(TLongObjectMap<InternalLatestFacet.Entry> entries) {
        if (queue == null) {
            this.queue = new EntryPriorityQueue(start + size);
        }
        entries.forEachEntry(new TLongObjectProcedure<Entry>() {
            @Override
            public boolean execute(long key, Entry entry) {
                entry.key = key;
                queue.insertWithOverflow(entry);
                return true;
            }
        });
    }

    public static class Entry {
        public long ts;
        public int value;
        public long key;

        public Entry(long ts, int value) {
            this.ts = ts;
            this.value = value;
        }

        public Entry(long ts, int value, long key) {
            this.ts = ts;
            this.value = value;
            this.key = key;
        }
    }

    public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        for (Facet facet : facets) {
            InternalLatestFacet f = (InternalLatestFacet) facet;
            if (this == facet) {
                continue;
            }
            while (f.queue.size() > 0) {
                queue.insertWithOverflow(f.queue.pop());
            }
            this.total += f.total;
        }
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {

        builder.startObject(name);
        builder.field(Fields._TYPE, TYPE);
        builder.field(Fields.TOTAL, total);
        builder.startArray(Fields.ENTRIES);
        int num_entries = queue.size() - start;

        if (num_entries > 0) {
            Entry[] entries = new Entry[num_entries];
            for (int i = entries.length - 1; i >= 0; i--) {
                entries[i] = queue.pop();
            }
            for (int i = 0; i < entries.length; i++) {
                Entry e = entries[i];
                builder.startObject();
                builder.field(Fields.VALUE, e.value);
                builder.field(Fields.KEY, e.key);
                builder.field(Fields.TS, e.ts);
                builder.endObject();
            }
        }

        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalLatestFacet readDistinctTermsFacet(StreamInput in)
            throws IOException {
        InternalLatestFacet facet = new InternalLatestFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.name = in.readString();
        this.size = in.readVInt();
        this.start = in.readVInt();
        this.total = in.readVInt();
        this.queue = new EntryPriorityQueue(start + size);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            queue.insertWithOverflow(new Entry(in.readVLong(), in.readVInt(),
                    in.readVLong()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeVInt(size);
        out.writeVInt(start);
        out.writeVInt(total);
        out.writeVInt(queue.size());
        while (queue.size() > 0) {
            Entry e = queue.pop();
            out.writeVLong(e.ts);
            out.writeVInt(e.value);
            out.writeVLong(e.key);
        }
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString(
                "_type");
        static final XContentBuilderString TS = new XContentBuilderString("ts");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString ENTRIES = new XContentBuilderString(
                "entries");
        static final XContentBuilderString KEY = new XContentBuilderString(
                "key");
        static final XContentBuilderString VALUE = new XContentBuilderString(
                "value");
    }
}
