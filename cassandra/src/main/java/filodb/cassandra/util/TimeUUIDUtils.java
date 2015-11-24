package filodb.cassandra.util;

import java.nio.ByteBuffer;

/**
 * User: satya
 */
public class TimeUUIDUtils {

    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    /**
     * Gets a new and unique time uuid in milliseconds. It is useful to use in a TimeUUIDType sorted column family.
     *
     * @return the time uuid
     */
    public static java.util.UUID getUniqueTimeUUIDinMillis() {
        return new java.util.UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
    }

    /**
     * Gets a new time uuid based on <code>time<code>.
     * NOTE: this algorithm does not resolve duplicates. To avoid duplicates use
     * It is useful to use in a TimeUUIDType sorted column family.
     *
     * @return the time uuid
     */
    public static java.util.UUID getTimeUUID(long time) {
        return new java.util.UUID(createTime(time), UUIDGen.getClockSeqAndNode());
    }

    private static long createTime(long currentTime) {
        long time;

        // UTC time
        long timeToUse = (currentTime * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

        // time low
        time = timeToUse << 32;

        // time mid
        time |= (timeToUse & 0xFFFF00000000L) >> 16;

        // time hi and version
        time |= 0x1000 | ((timeToUse >> 48) & 0x0FFF); // version 1
        return time;
    }


    /**
     * Returns an instance of uuid. Useful for when you read out of cassandra
     * you are getting a byte[] that needs to be converted into a TimeUUID.
     *
     * @param uuid the uuid
     * @return the java.util.uuid
     */
    public static java.util.UUID toUUID(byte[] uuid) {
        return uuid(uuid, 0);
    }

    /**
     * Retrieves the time as long based on the byte[] representation of a UUID.
     *
     * @param uuid byte[] uuid representation
     * @return a long representing the time
     */
    public static long getTimeFromUUID(byte[] uuid) {
        return getTimeFromUUID(TimeUUIDUtils.toUUID(uuid));
    }

    public static long getTimeFromUUID(java.util.UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    /**
     * As byte array.
     *
     * @param uuid the uuid
     * @return the byte[]
     */
    public static byte[] asByteArray(java.util.UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[16];

        for (int i = 0; i < 8; i++) {
            buffer[i] = (byte) (msb >>> 8 * (7 - i));
        }

        for (int i = 8; i < 16; i++) {
            buffer[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return buffer;
    }

    /**
     * Coverts a java.util.UUID into a ByteBuffer.
     *
     * @param uuid a java.util.UUID
     * @return a ByteBuffer representaion of the param UUID
     */
    public static ByteBuffer asByteBuffer(java.util.UUID uuid) {
        if (uuid == null) {
            return null;
        }

        return ByteBuffer.wrap(asByteArray(uuid));
    }


    public static java.util.UUID uuid(byte[] uuid, int offset) {
        ByteBuffer bb = ByteBuffer.wrap(uuid, offset, 16);
        return new java.util.UUID(bb.getLong(), bb.getLong());
    }

    /**
     * Converts a ByteBuffer containing a UUID into a java.util.UUID
     *
     * @param bb a ByteBuffer containing a UUID
     * @return a java.util.UUID
     */
    public static java.util.UUID uuid(ByteBuffer bb) {
        bb = bb.slice();
        return new java.util.UUID(bb.getLong(), bb.getLong());
    }

}
