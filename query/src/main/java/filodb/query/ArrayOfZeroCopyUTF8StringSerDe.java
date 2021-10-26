package filodb.query;

import filodb.memory.format.ZeroCopyUTF8String;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Serializer/Deserializer for sketches with ZeroCopyUTF8String data.
 *
 * (This code is copied nearly verbatim from the
 *     DataSketches ArrayOfStringsSerDe source code.)
 */
public class ArrayOfZeroCopyUTF8StringSerDe extends ArrayOfItemsSerDe<ZeroCopyUTF8String> {

    /* Note to future developers: use of the ZeroCopyUTF8String constructor
       directly will silently fail. Always use ZeroCopyUTF8String::apply.   */

    @Override
    public byte[] serializeToByteArray(final ZeroCopyUTF8String[] items) {
        int length = 0;
        final byte[][] itemsBytes = new byte[items.length][];
        for (int i = 0; i < items.length; i++) {
            itemsBytes[i] = items[i].bytes();
            length += itemsBytes[i].length + Integer.BYTES;
        }
        final byte[] bytes = new byte[length];
        final WritableMemory mem = WritableMemory.writableWrap(bytes);
        long offsetBytes = 0;
        for (int i = 0; i < items.length; i++) {
            mem.putInt(offsetBytes, itemsBytes[i].length);
            offsetBytes += Integer.BYTES;
            mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
            offsetBytes += itemsBytes[i].length;
        }
        return bytes;
    }

    @Override
    public ZeroCopyUTF8String[] deserializeFromMemory(final Memory mem, final int numItems) {
        final ZeroCopyUTF8String[] array = new ZeroCopyUTF8String[numItems];
        long offsetBytes = 0;
        for (int i = 0; i < numItems; i++) {
            Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
            final int strLength = mem.getInt(offsetBytes);
            offsetBytes += Integer.BYTES;
            final byte[] bytes = new byte[strLength];
            Util.checkBounds(offsetBytes, strLength, mem.getCapacity());
            mem.getByteArray(offsetBytes, bytes, 0, strLength);
            offsetBytes += strLength;
            array[i] = ZeroCopyUTF8String.apply(bytes, 0, strLength);
        }
        return array;
    }

}
