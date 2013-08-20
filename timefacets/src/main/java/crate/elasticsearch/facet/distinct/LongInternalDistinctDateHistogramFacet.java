package crate.elasticsearch.facet.distinct;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.facet.Facet;

import java.io.IOException;
import java.util.*;

public class LongInternalDistinctDateHistogramFacet extends InternalDistinctDateHistogramFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray("LongDistinctDateHistogram".getBytes());

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    LongInternalDistinctDateHistogramFacet() {
    }

    LongInternalDistinctDateHistogramFacet(String name) {
        super(name);
    }

    public LongInternalDistinctDateHistogramFacet(String name, ComparatorType comparatorType, ExtTLongObjectHashMap<DistinctEntry> entries, boolean cachedEntries) {
        super(name);
        this.comparatorType = comparatorType;
        this.tEntries = entries;
        this.cachedEntries = cachedEntries;
        this.entries = entries.valueCollection();
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    public static LongInternalDistinctDateHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        LongInternalDistinctDateHistogramFacet facet = new LongInternalDistinctDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    protected  LongInternalDistinctDateHistogramFacet newFacet() {
        return new LongInternalDistinctDateHistogramFacet(getName());
    }

    /**
     * The reader for the internal transport protocol.
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        comparatorType = ComparatorType.fromId(in.readByte());

        cachedEntries = false;
        int size = in.readVInt();
        entries = new ArrayList<DistinctEntry>(size);
        for (int i = 0; i < size; i++) {
            long time = in.readLong();
            int nameSize = in.readVInt();
            Set<Object> names = new HashSet<Object>(nameSize);
            for (int j = 0; j < nameSize; j++) {
                names.add(in.readLong());
            }
            entries.add(new DistinctEntry(time, names));
        }
    }


    /**
     * The writer for the internal transport protocol.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(comparatorType.id());
        out.writeVInt(entries.size());
        for (DistinctEntry entry : entries) {
            out.writeLong(entry.getTime());
            out.writeVInt(entry.getValues().size());
            for (Object name : entry.getValues()) {
                out.writeLong((Long) name);
            }
        }
        releaseCache();
    }
}
