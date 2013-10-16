package com.carrotsearch.junitbenchmarks;

import java.io.*;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * {@link com.carrotsearch.junitbenchmarks.IResultsConsumer} that writes XML files for each benchmark.
 */
public final class XMLConsumer extends AutocloseConsumer implements Closeable
{
    /**
     * Timestamp format.
     */
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    /**
     * Output XML writer.
     */
    private Writer writer;

    /**
     * Internal buffer for appends.
     */
    private final StringBuilder b = new StringBuilder();

    /**
     * Number format.
     */
    private final NumberFormat nf;

    /**
     * Instantiate from global options. 
     */
    public XMLConsumer() throws IOException
    {
        this(getDefaultOutputFile());
    }

    /*
     * 
     */
    public XMLConsumer(File fileName) throws IOException
    {
        writer = new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8");
        addAutoclose(this);
        writer.write("<benchmark-results tstamp=\"" + tstamp() + "\">\n\n");

        nf = NumberFormat.getInstance(Locale.ENGLISH);
        nf.setMaximumFractionDigits(3);
        nf.setGroupingUsed(false);            
    }

    /**
     * Accept a single benchmark result.
     */
    public void accept(Result result) throws IOException
    {
        // We emit XML by hand. If anything more difficult comes up, we can switch
        // to SimpleXML or some other XML binding solution.
        b.setLength(0);
        b.append("\t<testname");
        attribute(b, "classname", result.getTestClassName());
        attribute(b, "name", result.getTestMethodName());

        b.append("\n\t\t");
        attribute(b, "benchmark-rounds", Integer.toString(result.benchmarkRounds));
        attribute(b, "warmup-rounds", Integer.toString(result.warmupRounds));

        b.append("\n\t\t");
        attribute(b, "round-avg", nf.format(result.roundAverage.avg));
        attribute(b, "round-stddev", nf.format(result.roundAverage.stddev));

        b.append("\n\t\t");
        attribute(b, "gc-avg", nf.format(result.gcAverage.avg));
        attribute(b, "gc-stddev", nf.format(result.gcAverage.stddev));

        b.append("\n\t\t");
        attribute(b, "gc-invocations", Long.toString(result.gcInfo.accumulatedInvocations()));
        attribute(b, "gc-time", nf.format(result.gcInfo.accumulatedTime() / 1000.0));

        b.append("\n\t\t");
        attribute(b, "benchmark-time-total", nf.format(result.benchmarkTime * 0.001));
        attribute(b, "warmup-time-total", nf.format(result.warmupTime * 0.001));

        b.append("\n\t\t");
        attribute(b, "threads", Integer.toString(result.getThreadCount()));

        b.append("/>\n\n");

        writer.write(b.toString());
        writer.flush();
    }

    /** 
     * Close the output XML stream.
     */
    public void close()
    {
        try
        {
            if (this.writer != null)
            {
                writer.write("</benchmark-results>");
                writer.close();
                writer = null;
                removeAutoclose(this);
            }
        }
        catch (IOException e)
        {
            // Ignore.
        }
    }

    /**
     * Returns the default output file.
     */
    private static File getDefaultOutputFile()
    {
        final String xmlPath = System.getProperty(BenchmarkOptionsSystemProperties.XML_FILE_PROPERTY);
        if (xmlPath != null && !xmlPath.trim().equals(""))
        {
            return new File(xmlPath);
        }
    
        throw new IllegalArgumentException("Missing global property: "
            + BenchmarkOptionsSystemProperties.XML_FILE_PROPERTY); 
    }

    /**
     * Unique timestamp for this XML consumer. 
     */
    private static String tstamp()
    {
        SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_FORMAT);
        return sdf.format(new Date());
    }

    /**
     * Append an attribute to XML.
     */
    private void attribute(StringBuilder b, String attrName, String value)
    {
        b.append(' ');
        b.append(attrName);
        b.append("=\"");
        b.append(Escape.xmlAttrEscape(value));
        b.append('"');
    }
}
