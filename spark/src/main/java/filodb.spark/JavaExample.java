package filodb.spark;

import org.apache.spark.sql.SQLContext;

class JavaExample {
  void callIntoScala(SQLContext sql) {
    FiloContext ctx = new FiloContext(sql);
    ctx.filoDataset("foo");
  }
}