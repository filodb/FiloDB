package filodb.memory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;

import java.util.List;

/**
 * Simple utility which generates a report of all memory managed by the JVM.
 */
public class JavaMemoryInfo {
    /**
     * Generates a report that looks like so:
     *
     * Total overall usage: committed=348717056(332M), max=5341446144(5094M)
     * - Total heap usage: committed=335544320(320M), max=5341446144(5094M)
     *   - G1 Eden Space: committed=26214400(25M), max=undefined
     *   - G1 Old Gen: committed=309329920(295M), max=5341446144(5094M)
     *   - G1 Survivor Space: committed=0(0), max=undefined
     * - Total non-heap usage: committed=13172736(12864K), max=undefined
     *   - CodeHeap 'non-nmethods': committed=2555904(2496K), max=5898240(5760K)
     *   - Metaspace: committed=4980736(4864K), max=undefined
     *   - CodeHeap 'profiled nmethods': committed=2555904(2496K), max=122880000(117M)
     *   - Compressed Class Space: committed=524288(512K), max=1073741824(1024M)
     *   - CodeHeap 'non-profiled nmethods': committed=2555904(2496K), max=122880000(117M)
     *
     * Committed memory is how much the JVM has currently allocated from the OS, although it
     * might not be using all of it at the moment.
     */
    public static String generate() {
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        List<MemoryPoolMXBean> memPoolBeans = ManagementFactory.getMemoryPoolMXBeans();

        var b = new StringBuilder();

        MemoryUsage total = sum(memBean.getHeapMemoryUsage(), memBean.getNonHeapMemoryUsage());
        b.append("Total overall usage: ")
            .append(usageInfo(total)).append('\n');

        b.append("- Total heap usage: ")
            .append(usageInfo(memBean.getHeapMemoryUsage())).append('\n');

        for (var memPool : memPoolBeans) {
            if (memPool.isValid() && memPool.getType() == MemoryType.HEAP) {
                b.append("  - ").append(poolInfo(memPool)).append('\n');
            }
        }

        b.append("- Total non-heap usage: ")
            .append(usageInfo(memBean.getNonHeapMemoryUsage())).append('\n');

        for (var memPool : memPoolBeans) {
            if (memPool.isValid() && memPool.getType() == MemoryType.NON_HEAP) {
                b.append("  - ").append(poolInfo(memPool)).append('\n');
            }
        }

        return b.toString();
    }

    private static String poolInfo(MemoryPoolMXBean memPool) {
        return memPool.getName() + ": " + usageInfo(memPool.getUsage());
    }

    private static String usageInfo(MemoryUsage usage) {
        return "committed=" + sizeInfo(usage.getCommitted()) + ", max=" + sizeInfo(usage.getMax());
    }

    private static String sizeInfo(long size) {
        if (size < 0) {
            return "undefined";
        }

        long rsize;

        String suffix;
        if (size < 1024L * 20) {
            rsize = size;
            suffix = "";
        } else if (size < 1024L * 1024 * 20) {
            rsize = size / 1024L;
            suffix = "K";
        } else if (size < 1024L * 1024 * 1024 * 20) {
            rsize = size / (1024 * 1024L);
            suffix = "M";
        } else {
            rsize = size / (1024 * 1024 * 1024L);
            suffix = "G";
        }

        return size + "(" + rsize + suffix + ")";
    }

    private static MemoryUsage sum(MemoryUsage a, MemoryUsage b) {
        return new MemoryUsage(sum(a.getInit(), b.getInit()),
                               sum(a.getUsed(), b.getUsed()),
                               sum(a.getCommitted(), b.getCommitted()),
                               sum(a.getMax(), b.getMax()));
    }

    private static long sum(long a, long b) {
        return a < 0 ? b : (b < 0 ? a : (a + b));
    }
}
