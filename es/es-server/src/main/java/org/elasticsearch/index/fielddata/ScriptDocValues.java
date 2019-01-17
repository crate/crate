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

package org.elasticsearch.index.fielddata;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import static org.elasticsearch.common.Booleans.parseBoolean;

/**
 * Script level doc values, the assumption is that any implementation will
 * implement a <code>getValue</code> and a <code>getValues</code> that return
 * the relevant type that then can be used in scripts.
 *
 * Implementations should not internally re-use objects for the values that they
 * return as a single {@link ScriptDocValues} instance can be reused to return
 * values form multiple documents.
 */
public abstract class ScriptDocValues<T> extends AbstractList<T> {

    static final boolean EXCEPTION_FOR_MISSING_VALUE =
        parseBoolean(System.getProperty("es.scripting.exception_for_missing_value", "false"));

    /**
     * Set the current doc ID.
     */
    public abstract void setNextDocId(int docId) throws IOException;

    /**
     * Return a copy of the list of the values for the current document.
     */
    public final List<T> getValues() {
        return this;
    }

    // Throw meaningful exceptions if someone tries to modify the ScriptDocValues.
    @Override
    public final void add(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final T set(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    public static final class Longs extends ScriptDocValues<Long> {
        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Longs.class));
        private final SortedNumericDocValues in;
        /**
         * Callback for deprecated fields. In production this should always point to
         * {@link #deprecationLogger} but tests will override it so they can test that
         * we use the required permissions when calling it.
         */
        private final BiConsumer<String, String> deprecationCallback;
        private long[] values = new long[0];
        private int count;
        private Dates dates;
        private int docId = -1;

        /**
         * Standard constructor.
         */
        public Longs(SortedNumericDocValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        /**
         * Constructor for testing the deprecation callback.
         */
        Longs(SortedNumericDocValues in, BiConsumer<String, String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            this.docId = docId;
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
            if (dates != null) {
                dates.setNextDocId(docId);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public SortedNumericDocValues getInternalValues() {
            return this.in;
        }

        public long getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return 0L;
            }
            return values[0];
        }

        @Deprecated
        public JodaCompatibleZonedDateTime getDate() throws IOException {
            deprecated("scripting_get_date_deprecation","getDate on numeric fields is deprecated. Use a date field to get dates.");
            if (dates == null) {
                dates = new Dates(in, deprecationCallback);
                dates.setNextDocId(docId);
            }
            return dates.getValue();
        }

        @Deprecated
        public List<JodaCompatibleZonedDateTime> getDates() throws IOException {
            deprecated("scripting_get_date_deprecation", "getDates on numeric fields is deprecated. Use a date field to get dates.");
            if (dates == null) {
                dates = new Dates(in, deprecationCallback);
                dates.setNextDocId(docId);
            }
            return dates;
        }

        @Override
        public Long get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }
    }

    public static final class Dates extends ScriptDocValues<JodaCompatibleZonedDateTime> {

        private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Dates.class));

        private final JodaCompatibleZonedDateTime EPOCH = new JodaCompatibleZonedDateTime(Instant.EPOCH, ZoneOffset.UTC);

        private final SortedNumericDocValues in;

        private final BiConsumer<String, String> deprecationCallback;

        /**
         * Values wrapped in {@link java.time.ZonedDateTime} objects.
         */
        private JodaCompatibleZonedDateTime[] dates;
        private int count;

        /**
         * Standard constructor.
         */
        public Dates(SortedNumericDocValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        /**
         * Constructor for testing with a deprecation callback.
         */
        Dates(SortedNumericDocValues in, BiConsumer<String, String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public JodaCompatibleZonedDateTime getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return EPOCH;
            }
            return get(0);
        }

        /**
         * Fetch the first value. Added for backwards compatibility with 5.x when date fields were {@link Longs}.
         */
        @Deprecated
        public JodaCompatibleZonedDateTime getDate() {
            deprecated("scripting_get_date_deprecation", "getDate is no longer necessary on date fields as the value is now a date.");
            return getValue();
        }

        /**
         * Fetch all the values. Added for backwards compatibility with 5.x when date fields were {@link Longs}.
         */
        @Deprecated
        public List<JodaCompatibleZonedDateTime> getDates() {
            deprecated("scripting_get_date_deprecation", "getDates is no longer necessary on date fields as the values are now dates.");
            return this;
        }

        @Override
        public JodaCompatibleZonedDateTime get(int index) {
            if (index >= count) {
                throw new IndexOutOfBoundsException(
                        "attempted to fetch the [" + index + "] date when there are only ["
                                + count + "] dates.");
            }
            return dates[index];
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                count = in.docValueCount();
            } else {
                count = 0;
            }
            refreshArray();
        }

        /**
         * Refresh the backing array. Package private so it can be called when {@link Longs} loads dates.
         */
        void refreshArray() throws IOException {
            if (count == 0) {
                return;
            }
            if (dates == null || count > dates.length) {
                // Happens for the document. We delay allocating dates so we can allocate it with a reasonable size.
                dates = new JodaCompatibleZonedDateTime[count];
            }
            for (int i = 0; i < count; ++i) {
                dates[i] = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(in.nextValue()), ZoneOffset.UTC);
            }
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }
    }

    public static final class Doubles extends ScriptDocValues<Double> {

        private final SortedNumericDoubleValues in;
        private double[] values = new double[0];
        private int count;

        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Doubles.class));
        private final BiConsumer<String, String> deprecationCallback;

        public Doubles(SortedNumericDoubleValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        public Doubles(SortedNumericDoubleValues in, BiConsumer<String, String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public SortedNumericDoubleValues getInternalValues() {
            return this.in;
        }

        public double getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return 0d;
            }
            return values[0];
        }

        @Override
        public Double get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }
    }

    public static final class GeoPoints extends ScriptDocValues<GeoPoint> {

        private final MultiGeoPointValues in;
        private GeoPoint[] values = new GeoPoint[0];
        private int count;

        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(GeoPoints.class));
        private final BiConsumer<String, String> deprecationCallback;

        public GeoPoints(MultiGeoPointValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        public GeoPoints(MultiGeoPointValues in, BiConsumer<String, String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    GeoPoint point = in.nextValue();
                    values[i] = new GeoPoint(point.lat(), point.lon());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                values[i] = new GeoPoint();
                }
            }
        }

        public GeoPoint getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return null;
            }
            return values[0];
        }

        public double getLat() {
            return getValue().lat();
        }

        public double[] getLats() {
            List<GeoPoint> points = getValues();
            double[] lats = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lats[i] = points.get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            List<GeoPoint> points = getValues();
            double[] lons = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lons[i] = points.get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public GeoPoint get(int index) {
            final GeoPoint point = values[index];
            return new GeoPoint(point.lat(), point.lon());
        }

        @Override
        public int size() {
            return count;
        }

        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), lat, lon);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return arcDistance(lat, lon);
        }

        public double planeDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.planeDistance(point.lat(), point.lon(), lat, lon);
        }

        public double planeDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return planeDistance(lat, lon);
        }

        public double geohashDistance(String geohash) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), GeoHashUtils.decodeLatitude(geohash),
                GeoHashUtils.decodeLongitude(geohash));
        }

        public double geohashDistanceWithDefault(String geohash, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return geohashDistance(geohash);
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }
    }

    public static final class Booleans extends ScriptDocValues<Boolean> {

        private final SortedNumericDocValues in;
        private boolean[] values = new boolean[0];
        private int count;

        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Booleans.class));
        private final BiConsumer<String, String> deprecationCallback;

        public Booleans(SortedNumericDocValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        public Booleans(SortedNumericDocValues in, BiConsumer<String, String> deprecationCallback) {
            this.in = in;
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue() == 1;
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = grow(values, count);
        }

        public boolean getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return false;
            }
            return values[0];
        }

        @Override
        public Boolean get(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        private static boolean[] grow(boolean[] array, int minSize) {
            assert minSize >= 0 : "size must be positive (got " + minSize
                    + "): likely integer overflow?";
            if (array.length < minSize) {
                return Arrays.copyOf(array, ArrayUtil.oversize(minSize, 1));
            } else
                return array;
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }

    }

    abstract static class BinaryScriptDocValues<T> extends ScriptDocValues<T> {

        private final SortedBinaryDocValues in;
        protected BytesRefBuilder[] values = new BytesRefBuilder[0];
        protected int count;

        BinaryScriptDocValues(SortedBinaryDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    // We need to make a copy here, because BytesBinaryDVAtomicFieldData's SortedBinaryDocValues
                    // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                    // instance for all slots in the values array.
                    values[i].copyBytes(in.nextValue());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                final int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        @Override
        public int size() {
            return count;
        }

    }

    public static final class Strings extends BinaryScriptDocValues<String> {

        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Strings.class));
        private final BiConsumer<String, String> deprecationCallback;

        public Strings(SortedBinaryDocValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        public Strings(SortedBinaryDocValues in, BiConsumer<String, String> deprecationCallback) {
            super(in);
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public String get(int index) {
            return values[index].get().utf8ToString();
        }

        public String getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return null;
            }
            return get(0);
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }
    }

    public static final class BytesRefs extends BinaryScriptDocValues<BytesRef> {

        protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Strings.class));
        private final BiConsumer<String, String> deprecationCallback;

        public BytesRefs(SortedBinaryDocValues in) {
            this(in, (key, message) -> deprecationLogger.deprecatedAndMaybeLog(key, message));
        }

        public BytesRefs(SortedBinaryDocValues in, BiConsumer<String, String> deprecationCallback) {
            super(in);
            this.deprecationCallback = deprecationCallback;
        }

        @Override
        public BytesRef get(int index) {
            /**
             * We need to make a copy here because {@link BinaryScriptDocValues} might reuse the
             * returned value and the same instance might be used to
             * return values from multiple documents.
             **/
            return values[index].toBytesRef();
        }

        public BytesRef getValue() {
            if (count == 0) {
                if (ScriptDocValues.EXCEPTION_FOR_MISSING_VALUE) {
                    throw new IllegalStateException("A document doesn't have a value for a field! " +
                        "Use doc[<field>].size()==0 to check if a document is missing a field!");
                }
                deprecated("scripting_missing_value_deprecation",
                    "returning default values for missing document values is deprecated. " +
                    "Set system property '-Des.scripting.exception_for_missing_value=true' "  +
                    "to make behaviour compatible with future major versions!");
                return new BytesRef();
            }
            return get(0);
        }

        /**
         * Log a deprecation log, with the server's permissions, not the permissions of the
         * script calling this method. We need to do this to prevent errors when rolling
         * the log file.
         */
        private void deprecated(String key, String message) {
            // Intentionally not calling SpecialPermission.check because this is supposed to be called by scripts
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    deprecationCallback.accept(key, message);
                    return null;
                }
            });
        }

    }
}
