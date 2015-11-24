package filodb.cassandra.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * User: satya
 */
public class UUIDGen {

    /**
     * No instances needed.
     */
    private UUIDGen() {
        super();
    }

    /**
     * The last time value. Used to remove duplicate UUIDs.
     */
    private static long lastTime = Long.MIN_VALUE;

    /**
     * The cached MAC address.
     */
    private static String macAddress = null;

    /**
     * The current clock and node value.
     */
    private static long clockSeqAndNode = 0x8000000000000000L;

    static {

        try {
            Class.forName("java.net.InterfaceAddress");
            macAddress = Class.forName(
                    "filodb.cassandra.util.UUIDGen$HardwareAddressLookup").newInstance().toString();
        } catch (ExceptionInInitializerError err) {
            // Ignored.
        } catch (ClassNotFoundException ex) {
            // Ignored.
        } catch (LinkageError err) {
            // Ignored.
        } catch (IllegalAccessException ex) {
            // Ignored.
        } catch (InstantiationException ex) {
            // Ignored.
        } catch (SecurityException ex) {
            // Ignored.
        }

        if (macAddress == null) {

            Process p = null;
            BufferedReader in = null;

            try {
                String osname = System.getProperty("os.name", "");

                if (osname.startsWith("Windows")) {
                    p = Runtime.getRuntime().exec(
                            new String[]{"ipconfig", "/all"}, null);
                }
                // Solaris code must appear before the generic code
                else if (osname.startsWith("Solaris")
                        || osname.startsWith("SunOS")) {
                    String hostName = getFirstLineOfCommand(
                            "uname", "-n");
                    if (hostName != null) {
                        p = Runtime.getRuntime().exec(
                                new String[]{"/usr/sbin/arp", hostName},
                                null);
                    }
                } else if (new File("/usr/sbin/lanscan").exists()) {
                    p = Runtime.getRuntime().exec(
                            new String[]{"/usr/sbin/lanscan"}, null);
                } else if (new File("/sbin/ifconfig").exists()) {
                    p = Runtime.getRuntime().exec(
                            new String[]{"/sbin/ifconfig", "-a"}, null);
                }

                if (p != null) {
                    in = new BufferedReader(new InputStreamReader(
                            p.getInputStream()), 128);
                    String l = null;
                    while ((l = in.readLine()) != null) {
                        macAddress = MACAddressParser.parse(l);
                        if (macAddress != null
                                && Hex.parseShort(macAddress) != 0xff) {
                            break;
                        }
                    }
                }

            } catch (SecurityException ex) {
                // Ignore it.
            } catch (IOException ex) {
                // Ignore it.
            } finally {
                if (p != null) {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException ex) {
                            // Ignore it.
                        }
                    }
                    try {
                        p.getErrorStream().close();
                    } catch (IOException ex) {
                        // Ignore it.
                    }
                    try {
                        p.getOutputStream().close();
                    } catch (IOException ex) {
                        // Ignore it.
                    }
                    p.destroy();
                }
            }

        }

        if (macAddress != null) {
            clockSeqAndNode |= Hex.parseLong(macAddress);
        } else {
            try {
                byte[] local = InetAddress.getLocalHost().getAddress();
                clockSeqAndNode |= (local[0] << 24) & 0xFF000000L;
                clockSeqAndNode |= (local[1] << 16) & 0xFF0000;
                clockSeqAndNode |= (local[2] << 8) & 0xFF00;
                clockSeqAndNode |= local[3] & 0xFF;
            } catch (UnknownHostException ex) {
                clockSeqAndNode |= (long) (Math.random() * 0x7FFFFFFF);
            }
        }

        // Skip the clock sequence generation process and use random instead.

        clockSeqAndNode |= (long) (Math.random() * 0x3FFF) << 48;

    }

    /**
     * Returns the current clockSeqAndNode value.
     *
     * @return the clockSeqAndNode value
     */
    public static long getClockSeqAndNode() {
        return clockSeqAndNode;
    }

    /**
     * Generates a new time field. Each time field is unique and larger than the
     * previously generated time field.
     *
     * @return a new time value
     */
    public static long newTime() {
        return createTime(System.currentTimeMillis());
    }

    /**
     * Creates a new time field from the given timestamp. Note that even identical
     * values of <code>currentTimeMillis</code> will produce different time fields.
     *
     * @param currentTimeMillis the timestamp
     * @return a new time value
     */
    public static synchronized long createTime(long currentTimeMillis) {

        long time;

        // UTC time

        long timeMillis = (currentTimeMillis * 10000) + 0x01B21DD213814000L;

        if (timeMillis > lastTime) {
            lastTime = timeMillis;
        } else {
            timeMillis = ++lastTime;
        }

        // time low

        time = timeMillis << 32;

        // time mid

        time |= (timeMillis & 0xFFFF00000000L) >> 16;

        // time hi and version

        time |= 0x1000 | ((timeMillis >> 48) & 0x0FFF); // version 1

        return time;

    }

    /**
     * Returns the MAC address. Not guaranteed to return anything.
     *
     * @return the MAC address, may be <code>null</code>
     */
    public static String getMACAddress() {
        return macAddress;
    }

    /**
     * Returns the first line of the shell command.
     *
     * @param commands the commands to run
     * @return the first line of the command
     * @throws IOException
     */
    static String getFirstLineOfCommand(String... commands) throws IOException {

        Process p = null;
        BufferedReader reader = null;

        try {
            p = Runtime.getRuntime().exec(commands);
            reader = new BufferedReader(new InputStreamReader(
                    p.getInputStream()), 128);

            return reader.readLine();
        } finally {
            if (p != null) {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ex) {
                        // Ignore it.
                    }
                }
                try {
                    p.getErrorStream().close();
                } catch (IOException ex) {
                    // Ignore it.
                }
                try {
                    p.getOutputStream().close();
                } catch (IOException ex) {
                    // Ignore it.
                }
                p.destroy();
            }
        }

    }

    /**
     * Scans MAC addresses for good ones.
     */
    static class HardwareAddressLookup {

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            String out = null;
            try {
                Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
                if (ifs != null) {
                    while (ifs.hasMoreElements()) {
                        NetworkInterface iface = ifs.nextElement();
                        byte[] hardware = iface.getHardwareAddress();
                        if (hardware != null && hardware.length == 6
                                && hardware[1] != (byte) 0xff) {
                            out = Hex.append(new StringBuilder(36), hardware).toString();
                            break;
                        }
                    }
                }
            } catch (SocketException ex) {
                // Ignore it.
            }
            return out;
        }

    }

    private static class MACAddressParser {

        /**
         * No instances needed.
         */
        private MACAddressParser() {
            super();
        }

        /**
         * Attempts to find a pattern in the given String.
         *
         * @param in the String, may not be <code>null</code>
         * @return the substring that matches this pattern or <code>null</code>
         */
        static String parse(String in) {

            String out = in;

            // lanscan

            int hexStart = out.indexOf("0x");
            if (hexStart != -1 && out.indexOf("ETHER") != -1) {
                int hexEnd = out.indexOf(' ', hexStart);
                if (hexEnd > hexStart + 2) {
                    out = out.substring(hexStart, hexEnd);
                }
            } else {

                int octets = 0;
                int lastIndex, old, end;

                if (out.indexOf('-') > -1) {
                    out = out.replace('-', ':');
                }

                lastIndex = out.lastIndexOf(':');

                if (lastIndex > out.length() - 2) {
                    out = null;
                } else {

                    end = Math.min(out.length(), lastIndex + 3);

                    ++octets;
                    old = lastIndex;
                    while (octets != 5 && lastIndex != -1 && lastIndex > 1) {
                        lastIndex = out.lastIndexOf(':', --lastIndex);
                        if (old - lastIndex == 3 || old - lastIndex == 2) {
                            ++octets;
                            old = lastIndex;
                        }
                    }

                    if (octets == 5 && lastIndex > 1) {
                        out = out.substring(lastIndex - 2, end).trim();
                    } else {
                        out = null;
                    }

                }

            }

            if (out != null && out.startsWith("0x")) {
                out = out.substring(2);
            }

            return out;
        }

    }
}
