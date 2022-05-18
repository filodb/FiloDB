package filodb.core.memstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class DownsampleIndexCheckpointer {

    private static Logger log =  LoggerFactory.getLogger(DownsampleIndexCheckpointer.class);
    public static String CHECKPOINT_FILE_NAME = "filodb_checkpoint.properties";
    private static String CHECKPOINT_MILLIS = "checkpoint_millis";

    public static long getDownsampleLastCheckpointTime(File indexPath) {
        File checkpointFile = getCheckpointFile(indexPath);
        Properties props = new Properties();
        if (!checkpointFile.exists()) {
            return 0;
        }
        Reader r = null;
        try {
            r = new FileReader(checkpointFile);
            props.load(r);
            r.close();
        } catch (IOException ioe) {
            log.error("Error reading checkpoint file", ioe);
            return 0;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException ioe) {
                    //nothing to do
                    log.error("Failed to close checkpoint file ", ioe);
                }
            }
        }
        if (props.containsKey(CHECKPOINT_MILLIS)) {
            long millis = Long.parseLong(props.getProperty(CHECKPOINT_MILLIS));
            return millis;
        }
        return 0;
    }

    public static void writeCheckpoint(File indexPath, long millis) throws IOException {
        Properties props = new Properties();
        props.setProperty(CHECKPOINT_MILLIS, Long.toString(millis));
        File checkpointFile = getCheckpointFile(indexPath);
        Writer w = null;
        try {
            w = new FileWriter(checkpointFile);
            props.store(w, "Checkpoint update");
        } finally {
            try {
                w.close();
            } catch (IOException ioe) {
                //nothing to do
                log.error("Failed to close checkpoint file ", ioe);
            }
        }
    }

    private static File getCheckpointFile(File indexPath) {
        return new File(indexPath, CHECKPOINT_FILE_NAME);
    }
}
