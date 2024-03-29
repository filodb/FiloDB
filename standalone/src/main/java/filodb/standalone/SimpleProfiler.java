package filodb.standalone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import java.lang.reflect.Method;

import java.time.Instant;

import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * Simple profiler which samples threads and periodically logs a report to a file. When the
 * process is cleanly shutdown, the profiler stops and reports what it has immediately. This
 * makes it possible to use a very long report interval without data loss.
 */
public class SimpleProfiler {

    private final static Logger logger  = LoggerFactory.getLogger(SimpleProfiler.class);

    /**
     * Launches a global profiler, based on config. Typically, these are nested under the
     * "filodb.profiler" path:
     *
     * sample-rate = 10ms
     * report-interval = 60s
     * top-count = 50
     * out-file = "filodb.prof"
     *
     * In order for profiling to be enabled, all of the above properies must be set except for
     * the file. If no file is provided, then a temporary file is created, whose name is logged.
     *
     * @return false if not configured
     * @throws IOException if file cannot be opened
     */
    public static boolean launch(Config config) throws IOException {
        try {
            boolean enableSimpleProfiler = config.getBoolean("enable-simple-profiler");
            if(enableSimpleProfiler) {
                long sampleRateMillis = config.getDuration("sample-rate", TimeUnit.MILLISECONDS);
                long reportIntervalSeconds = config.getDuration("report-interval", TimeUnit.SECONDS);
                int topCount = config.getInt("top-count");
                new SimpleProfiler(sampleRateMillis, reportIntervalSeconds, topCount).start();
                return true;
            } else {
                return false;
            }
        } catch (ConfigException e) {
            LoggerFactory.getLogger(SimpleProfiler.class).debug("Not profiling: " + e);
            return false;
        }
    }

    /**
     * @param fileName candidate file name; if null, a temp file is created
     */
    private static File selectProfilerFile(String fileName) throws IOException {
        if (fileName != null) {
            return new File(fileName);
        }
        File file = File.createTempFile("filodb.SimpleProfiler", ".txt");
        LoggerFactory.getLogger(SimpleProfiler.class).info
            ("Created temp file for profile reporting: " + file);
        return file;
    }

    private final long mSampleRateMillis;
    private final long mReportIntervalMillis;
    private final int mTopCount;

    private Sampler mSampler;
    private Thread mShutdownHook;

    private long mNextReportAtMillis;

    /**
     * @param sampleRateMillis how often to perform a thread dump (10 millis is good)
     * @param reportIntervalSeconds how often to write a report to the output stream
     * @param topCount number of methods to report
     * @param out where to write the report
     */
    public SimpleProfiler(long sampleRateMillis, long reportIntervalSeconds, int topCount)
    {
        mSampleRateMillis = sampleRateMillis;
        mReportIntervalMillis = reportIntervalSeconds * 1000;
        mTopCount = topCount;
    }

    /**
     * Start the profiler. Calling a second time does nothing unless stopped.
     */
    public synchronized void start() {
        if (mSampler == null) {
            Sampler s = new Sampler();
            s.start();
            mSampler = s;
            mNextReportAtMillis = System.currentTimeMillis() + mReportIntervalMillis;
            try {
                mShutdownHook = new Thread(this::shutdown);
                Runtime.getRuntime().addShutdownHook(mShutdownHook);
            } catch (Throwable e) {
                // Ignore.
                mShutdownHook = null;
            }
        }
    }

    /**
     * Stop the profiler. Calling a second time does nothing unless started again.
     */
    public void stop() {
        Sampler s;
        synchronized (this) {
            s = mSampler;
            if (s != null) {
                s.mShouldStop = true;
                s.interrupt();

                if (mShutdownHook != null) {
                    try {
                        Runtime.getRuntime().removeShutdownHook(mShutdownHook);
                    } catch (Throwable e) {
                        // Ignore.
                    } finally {
                        mShutdownHook = null;
                    }
                }
            }
        }

        if (s != null) {
            while (true) {
                try {
                    s.join();
                    break;
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
            synchronized (this) {
                if (mSampler == s) {
                    mSampler = null;
                }
            }
        }
    }

    private void analyze(Map<StackTraceElement, TraceCounter> samples,
                         Map<String, SummaryCounter> summaries,
                         ThreadInfo[] infos)
        throws IOException
    {
        for (ThreadInfo info : infos) {
            StackTraceElement[] trace = examine(info);
            if (trace == null) {
                continue;
            }

            StackTraceElement elem = trace[0];

            TraceCounter tc = samples.get(elem);
            if (tc == null) {
                tc = new TraceCounter(elem);
                samples.put(elem, tc);
            }
            tc.mValue++;

            // Choose the package name as the summary name.
            String summaryName;
            {
                String className = elem.getClassName();
                int ix = className.lastIndexOf('.');
                if (ix <= 0) {
                    // No package at all, so use the class name instead.
                    summaryName = className;
                } else {
                    summaryName = className.substring(0, ix);
                }
            }

            SummaryCounter sc = summaries.get(summaryName);
            if (sc == null) {
                sc = new SummaryCounter(summaryName);
                summaries.put(summaryName, sc);
            }
            sc.mValue++;
        }

        synchronized (this) {
            long now = System.currentTimeMillis();
            if (now >= mNextReportAtMillis && mSampler == Thread.currentThread()) {
                mNextReportAtMillis = Math.max(now, mNextReportAtMillis + mReportIntervalMillis);
                report(samples, summaries);
            }
        }
    }

    private void report(Map<StackTraceElement, TraceCounter> samples,
                        Map<String, SummaryCounter> summaries)
        throws IOException
    {
        if(logger.isInfoEnabled()) {
            int size = samples.size();
            if (size == 0) {
                return;
            }

            SummaryCounter[] allSummaries = new SummaryCounter[summaries.size()];
            summaries.values().toArray(allSummaries);
            Arrays.sort(allSummaries);

            double summarySum = 0;
            for (SummaryCounter sc : allSummaries) {
                summarySum += sc.mValue;
            }

            TraceCounter[] topTraces = new TraceCounter[size];
            samples.values().toArray(topTraces);
            Arrays.sort(topTraces);

            double traceSum = 0;
            for (TraceCounter tc : topTraces) {
                traceSum += tc.mValue;
            }

            int limit = Math.min(mTopCount, size);
            StringBuilder b = new StringBuilder((allSummaries.length + limit) * 80);

            b.append(Instant.now()).append(' ').append(getClass().getName()).append('\n');

            b.append("--- all profiled packages --- \n");

            for (SummaryCounter sc : allSummaries) {
                String percentStr = String.format("%1$7.3f%%", 100.0 * (sc.mValue / summarySum));
                b.append(percentStr).append(' ').append(sc.mName).append('\n');
            }

            b.append("--- top profiled methods --- \n");

            for (int i=0; i<limit; i++) {
                TraceCounter tc = topTraces[i];
                if (tc.mValue == 0) {
                    // No more to report.
                    break;
                }

                String percentStr = String.format("%1$7.3f%%", 100.0 * (tc.mValue / traceSum));
                b.append(percentStr);

                StackTraceElement elem = tc.mElem;
                b.append(' ').append(elem.getClassName()).append('.').append(elem.getMethodName());

                String fileName = elem.getFileName();
                int lineNumber = elem.getLineNumber();

                if (fileName == null) {
                    if (lineNumber >= 0) {
                        b.append("(:").append(lineNumber).append(')');
                    }
                } else {
                    b.append('(').append(fileName);
                    if (lineNumber >= 0) {
                        b.append(':').append(lineNumber);
                    }
                    b.append(')');
                }

                b.append('\n');

                // Reset for next report.
                tc.mValue = 0;
            }
            logger.info(b.toString());
        }
        // Clear for next report.
        summaries.clear();

    }


    /**
     * @return null if rejected
     */
    private StackTraceElement[] examine(ThreadInfo info) {
        // Reject the sampler thread itself.
        if (info.getThreadId() == Thread.currentThread().getId()) {
            return null;
        }

        // Reject threads which aren't doing any real work.
        if (!info.getThreadState().equals(Thread.State.RUNNABLE)) {
            return null;
        }

        // XXX: only profile threads which are query scheduler threads
        // if (!info.getThreadName().startsWith("query-sched")) {
        //     return null;
        // }

        StackTraceElement[] trace = info.getStackTrace();

        // Reject internal threads which have no trace at all.
        if (trace == null || trace.length == 0) {
            return null;
        }

        // Reject some special internal native methods which aren't actually running.

        StackTraceElement elem = trace[0];

        if (elem.isNativeMethod()) {
            String className = elem.getClassName();

            // Reject threads which appeared as doing work only because they unparked another
            // thread, effectively yielding due to priority boosting.
            if (className.endsWith("misc.Unsafe")) {
                if (elem.getMethodName().equals("unpark")) {
                    return null;
                }
                // Sometimes the thread state is runnable for this method. Filter it out.
                if (elem.getMethodName().equals("park")) {
                    return null;
                }
            }

            switch (className) {
            case "java.lang.Object":
                // Reject threads which appeared as doing work only because they notified
                // another thread, effectively yielding due to priority boosting.
                if (elem.getMethodName().startsWith("notify")) {
                    return null;
                }
                // Sometimes the thread state is runnable for this method. Filter it out.
                if (elem.getMethodName().equals("wait")) {
                    return null;
                }
                break;

            case "java.lang.ref.Reference":
                // Reject threads waiting for GC'd objects to clean up.
                return null;

            case "java.lang.Thread":
                /* Track yield, since it's used by the ChunkMap lock.
                // Reject threads which appeared as doing work only because they yielded.
                if (elem.getMethodName().equals("yield")) {
                    return null;
                }
                */
                // Sometimes the thread state is runnable for this method. Filter it out.
                if (elem.getMethodName().equals("sleep")) {
                    return null;
                }
                break;

            case "java.net.PlainSocketImpl":
                // Reject threads blocked while accepting sockets.
                if (elem.getMethodName().startsWith("accept") ||
                    elem.getMethodName().startsWith("socketAccept"))
                {
                    return null;
                }
                break;

            case "sun.nio.ch.ServerSocketChannelImpl":
                // Reject threads blocked while accepting sockets.
                if (elem.getMethodName().startsWith("accept")) {
                    return null;
                }
                break;

            case "java.net.SocketInputStream":
                // Reject threads blocked while reading sockets. This also rejects threads
                // which are actually reading, but it's more common for threads to block.
                if (elem.getMethodName().startsWith("socketRead")) {
                    return null;
                }
                break;

            case "sun.nio.ch.SocketDispatcher":
                // Reject threads blocked while reading sockets. This also rejects threads
                // which are actually reading, but it's more common for threads to block.
                if (elem.getMethodName().startsWith("read")) {
                    return null;
                }
                break;

            case "sun.nio.ch.EPoll":
                // Reject threads blocked while selecting sockets.
                if (elem.getMethodName().startsWith("wait")) {
                    return null;
                }
                break;

            case "sun.nio.ch.KQueue":
                // Reject threads blocked while selecting sockets.
                if (elem.getMethodName().startsWith("poll")) {
                    return null;
                }
                break;
            case "sun.nio.ch.KQueueArrayWrapper":
                // Reject threads blocked while selecting sockets.
                if (elem.getMethodName().startsWith("kevent")) {
                    return null;
                }
                break;

            case "sun.nio.ch.WindowsSelectorImpl$SubSelector":
                // Reject threads blocked while selecting sockets.
                if (elem.getMethodName().startsWith("poll")) {
                    return null;
                }
                break;
            }

            // Reject threads blocked while selecting sockets. Match just on method name here,
            // to capture multiple epoll library implementations.
            if (elem.getMethodName().startsWith("epollWait")) {
                return null;
            }
        }

        return trace;
    }

    private void shutdown() {
        synchronized (this) {
            mShutdownHook = null;
            mNextReportAtMillis = Long.MIN_VALUE;
        }
        stop();
    }

    private static class Counter implements Comparable<Counter> {
        long mValue;

        @Override
        public int compareTo(Counter other) {
            // Descending order.
            return Long.compare(other.mValue, mValue);
        }
    }

    private static class TraceCounter extends Counter {
        final StackTraceElement mElem;

        TraceCounter(StackTraceElement elem) {
            mElem = elem;
        }
    }

    private static class SummaryCounter extends Counter {
        final String mName;

        SummaryCounter(String name) {
            mName = name;
        }
    }

    private class Sampler extends Thread {
        private final Map<StackTraceElement, TraceCounter> mSamples;
        private final Map<String, SummaryCounter> mSummaries;

        volatile boolean mShouldStop;

        Sampler() {
            super(SimpleProfiler.class.getName());

            try {
                setDaemon(true);
                setPriority(Thread.MAX_PRIORITY);
            } catch (SecurityException e) {
                // Ignore.
            }

            mSamples = new HashMap<>();
            mSummaries = new HashMap<>();
        }

        @Override
        public void run() {
            ThreadMXBean tb = ManagementFactory.getThreadMXBean();

            /*
              The method we want to call is:
                dumpAllThreads​(boolean lockedMonitors, boolean lockedSynchronizers, int maxDepth)

              ...but it's only available in Java 10. When running an older version of Java,
              we're forced to capture full stack traces, which is much more expensive.
            */

            Method dumpMethod = null;
            try {
                dumpMethod = ThreadMXBean.class.getMethod
                    ("dumpAllThreads", boolean.class, boolean.class, int.class);
            } catch (NoSuchMethodException e) {
                // Oh well, we tried.
            }

            while (!mShouldStop) {
                try {
                    try {
                        Thread.sleep(mSampleRateMillis);
                    } catch (InterruptedException e) {
                        // Probably should report upon JVM shutdown and then stop.
                    }

                    ThreadInfo[] infos;
                    if (dumpMethod == null) {
                        // Use the slow version.
                        // lockedMonitors=false, lockedSynchronizers=false
                        infos = tb.dumpAllThreads(false, false);
                    } else {
                        // Use the fast version.
                        // lockedMonitors=false, lockedSynchronizers=false, maxDepth=1
                        infos = (ThreadInfo[]) dumpMethod.invoke(tb, false, false, 1);
                    }

                    analyze(mSamples, mSummaries, infos);
                } catch (InterruptedIOException e) {
                    // Probably should stop.
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getThreadGroup().uncaughtException(t, e);
                }
            }
        }
    }
}
