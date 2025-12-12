import filodb.prometheus.ast.Expression;
import filodb.prometheus.parse.Parser;
import jdk.jfr.consumer.RecordingFile;
import jdk.jfr.consumer.RecordedEvent;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Test {
    public static void main(String[] args) {

        Expression expression = Parser.parseQuery("foo{_ws_=\"bar\", _ns_=\"baz\", label=~\"preset([1-9]|10|11|12)\\\\.example\\\\.org/path.+\"}");
        System.out.println(expression);

//        Path path = Paths.get("/Users/amolnayak/FiloDB/heap_dump/JFR/async-filodb-raw-tsdb35-f5d499879-dmk4b.jfr");
//
//        try (RecordingFile recordingFile = new RecordingFile(path)) {
//            while (recordingFile.hasMoreEvents()) {
//                RecordedEvent event = recordingFile.readEvent();
//                if (event != null) {
//                    System.out.println("Event: " + event.getEventType().getName());
//                    System.out.println("Timestamp: " + event.getStartTime());
//
//                    // Access event fields
//                    event.getFields().forEach(field -> {
//                        System.out.println(field.getName() + ": " + event.getValue(field.getName()));
//                    });
//
//                    System.out.println("---");
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}