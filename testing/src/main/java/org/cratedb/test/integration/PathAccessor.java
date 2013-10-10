package org.cratedb.test.integration;


import com.google.common.base.Charsets;
import org.elasticsearch.common.io.Streams;

import java.io.*;

public class PathAccessor {

    public static String stringFromPath(String path, Class<?> aClass) throws IOException {
        return Streams.copyToString(new InputStreamReader(
                getInputStream(path, aClass),
                Charsets.UTF_8));
    }

    public static byte[] bytesFromPath(String path, Class<?> aClass) throws IOException {
        InputStream is = getInputStream(path, aClass);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Streams.copy(is, out);
        is.close();
        out.close();
        return out.toByteArray();
    }

    public static InputStream getInputStream(String path, Class<?> aClass) throws FileNotFoundException {
        InputStream is = aClass.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return is;
    }
}
