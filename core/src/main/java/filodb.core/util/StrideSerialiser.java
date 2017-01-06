package filodb.core.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * A String serializer whose byte representation allows for byte by byte comparisons (with no knowledge
 * it's a string)
 */
public class StrideSerialiser {
    public static final StrideSerialiser instance = new StrideSerialiser();

    static Function<byte[], String> deserFunc = bytes -> instance.fromBytes(ByteBuffer.wrap(bytes));

    private static int[] strideSizes = new int[]{8, 8, 16, 16, 32, 32, 64};
    private static int finalStrideSize = strideSizes[strideSizes.length - 1];
    private static int maxVariableStrLength;
    private static int maxVariableSerLength;
    /**
     * A map for which the greatest lower bound of s is the length of the serialized byte array
     * resulting from serializing s.
     */
    private static NavigableMap<Integer, Integer> strLenToSerLenMap = new TreeMap<>();

    static {
        for (int strideSize : strideSizes) {
            maxVariableSerLength += strideSize + 1;
            strLenToSerLenMap.put(maxVariableStrLength, maxVariableSerLength);
            maxVariableStrLength += strideSize;
        }
    }

    public String fromBytes(ByteBuffer buf) {
        StringBuilder builder = new StringBuilder();
        int chunk = 0;
        int bytesUsed;
        do {
            int chunkSize = sizeOfChunk(chunk);
            byte[] chunkBytes = new byte[chunkSize];
            buf.get(chunkBytes, 0, chunkSize);
            bytesUsed = buf.get();
            if (bytesUsed >= 0) {
                chunkBytes = Arrays.copyOf(chunkBytes, bytesUsed);
            }
            String str = new String(chunkBytes, StandardCharsets.UTF_8);
            builder.append(str);

            // should we continue or not?
            chunk++;
        } while (bytesUsed < 0);
        return builder.toString();
    }

    public ByteBuffer toBytes(String value) {
        ByteBuffer out = ByteBuffer.allocate(sizeInBytes(value));
        write(out, value);
        out.rewind();
        return out;
    }

    public int sizeInBytes(String value) {
        int length = value.length();
        if (length < maxVariableStrLength) {
            return strLenToSerLenMap.floorEntry(length).getValue();
        } else {
            int uniformLength = length - maxVariableStrLength;
            int numChunks = uniformLength / finalStrideSize;
            if (numChunks * finalStrideSize < uniformLength) {
                numChunks++;
            }
            return numChunks * (finalStrideSize + 1) + maxVariableSerLength;
        }
    }

    public void write(ByteBuffer buf, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int chunk = 0;
        int pointer = 0;
        while (pointer < bytes.length) {
            int chunkSize = sizeOfChunk(chunk);
            writeChunk(buf, bytes, pointer, chunkSize);
            pointer += chunkSize;

            // now write the continuation byte
            if (pointer < bytes.length) {
                writeContinue(buf);
            } else {
                writeStop(buf, bytes.length - pointer + chunkSize);
            }
            chunk++;
        }
    }

    /**
     * Negative number means keep calm and carry on.
     */
    private void writeContinue(ByteBuffer buf) {
        buf.put((byte) 0x80);
    }

    /**
     * Positive number menas 'this is how many bytes we used'
     */
    private void writeStop(ByteBuffer buf, int numBytesFilled) {
        buf.put((byte) numBytesFilled);
    }

    /**
     * Always write exactly chunkSize bytes; if we exceed the end of the array provided then write
     * 0x00.
     *
     * @param destBuf   destination byte buffer
     * @param srcBytes  source of bytes to transfer
     * @param pointer   where in the source bytes we currently are
     * @param chunkSize how many bytes to try to transfer.
     */
    private void writeChunk(ByteBuffer destBuf, byte[] srcBytes, int pointer, int chunkSize) {
        for (int i = 0; i < chunkSize; i++) {
            if (pointer < srcBytes.length) {
                destBuf.put(srcBytes[pointer++]);
            } else {
                destBuf.put((byte) 0);
            }
        }
    }

    private int sizeOfChunk(int chunk) {
        if (chunk < strideSizes.length) {
            return strideSizes[chunk];
        }
        return finalStrideSize;
    }
}
