package com.carrotsearch.junitbenchmarks;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Consumers that should be closed at shutdown (if not earlier).
 */
public abstract class AutocloseConsumer implements IResultsConsumer
{
    /**
     * A list of closeables to close at shutdown (if not closed earlier).
     */
    private static List<Closeable> autoclose = new ArrayList<Closeable>();

    /**
     * A shutdown agent closing {@link #autoclose}.
     */
    private static Thread shutdownAgent;

    protected AutocloseConsumer()
    {
        initShutdownAgent();
    }

    protected static synchronized void addAutoclose(Closeable c)
    {
        autoclose.add(c);
    }

    protected static synchronized void removeAutoclose(Closeable c)
    {
        try
        {
            while (autoclose.remove(c))
            {
                // repeat.
            }
            c.close();
        }
        catch (IOException e)
        {
            // Ignore.
        }
    }

    private static synchronized void initShutdownAgent()
    {
        if (shutdownAgent == null)
        {
            shutdownAgent = new Thread()
            {
                public void run()
                {
                    for (Closeable w : new ArrayList<Closeable>(autoclose))
                    {
                        try
                        {
                            w.close();
                        }
                        catch (IOException e)
                        {
                            // Ignore, not much to do.
                        }
                    }
                }
            };
            Runtime.getRuntime().addShutdownHook(shutdownAgent);
        }
    }
}