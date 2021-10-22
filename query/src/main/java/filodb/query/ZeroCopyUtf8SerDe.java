package filodb.query;

import filodb.memory.format.ZeroCopyUTF8String;
import jnr.x86asm.Mem;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import scala.Array;
import spire.algebra.Sign;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Needs to be written in Java :(    // TODO(a_theimer): explain
 *
 * (basically copied from ArrayOfStringsSerDe)
 */
public class ZeroCopyUtf8SerDe extends ArrayOfItemsSerDe<ZeroCopyUTF8String> {
    static final int NUM_SIZE_BYTES = Integer.BYTES;


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

//        ///////
//        ZeroCopyUTF8String[] arr = deserializeFromMemory(Memory.wrap(bytes), items.length);
//        for (int i = 0; i < items.length; ++i) {
//            ZeroCopyUTF8String sorig = items[i];
//            ZeroCopyUTF8String snew = arr[i];
//            System.out.println("FFFFFFFFFFFFFFFFFFFFFFFFF");
//            System.out.println(sorig);
//            System.out.println(snew);
//        }
//        ///////

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

//    public static void printBytes(byte[] arr) {
//        for (byte b : arr) {
//            System.out.print(Byte.toString(b) + ", ");
//        }
//        System.out.println();
//    }

//    public static void main(String[] args) {
//        ZeroCopyUtf8SerDe serde = new ZeroCopyUtf8SerDe();
//        ZeroCopyUTF8String[] fresh = {ZeroCopyUTF8String.apply("helllo")};
//        byte[] ser = serde.serializeToByteArray(fresh);
//        ZeroCopyUTF8String[] dirty = serde.deserializeFromMemory(Memory.wrap(ser), 1);
//        printBytes(fresh[0].bytes());
//        printBytes(ser);
//        printBytes(dirty[0].bytes());
//    }

//    @Override
//    public byte[] serializeToByteArray(ZeroCopyUTF8String[] items) {
//        List<Byte> result = new ArrayList<Byte>();
//        for (ZeroCopyUTF8String str : items) {
//            byte[] sizeBytes = ByteBuffer.allocate(NUM_SIZE_BYTES).putInt(str.numBytes()).array();
//            for (byte b : sizeBytes) {
//                result.add(b);
//            }
//            for (byte b : str.bytes()) {
//                result.add(b);
//            }
//        }
//        byte[] res = new byte[result.size()];
//        for (int i = 0; i < res.length; ++i) {
//            res[i] = result.get(i);
//        }
//        return res;
//    }
//
//    @Override
//    public ZeroCopyUTF8String[] deserializeFromMemory(Memory mem, int numItems) {
//        ZeroCopyUTF8String[] res = new ZeroCopyUTF8String[numItems];
//        long offset = 0;
//        for (int i = 0; i < numItems; ++i) {
//            // read the string size
//            int numBytes = mem.getInt(offset);
//            offset += NUM_SIZE_BYTES;
//
//            // read the string
//            byte[] strBytes = new byte[numBytes];
//            mem.getByteArray(offset, strBytes, 0, NUM_SIZE_BYTES);
//            res[i] = new ZeroCopyUTF8String(strBytes, 0, numBytes);
//            offset += numBytes;
//        }
//        return res;
//    }
}
