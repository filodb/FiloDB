package filodb.core.metadata

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.core._
import filodb.core.query.ColumnInfo

class SchemasSpec extends FunSpec with Matchers {
  import Column.ColumnType._
  import Dataset._
  import NamesTestData._

  describe("DataSchema") {
    it("should return NotNameColonType if column specifiers not name:type format") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "column2", Nil, "first")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldEqual ColumnErrors(Seq(NotNameColonType("column2")))
    }

    it("should return BadColumnParams if name:type:params portion not valid key=value pairs") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "column2:a:b", Nil, "first")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldBe a[ColumnErrors]
      val errors = resp1.swap.get.asInstanceOf[ColumnErrors].errs
      errors should have length 1
      errors.head shouldBe a[BadColumnParams]
    }

    it("should return BadColumnParams if required param config not specified") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "h:hist:foo=bar", Nil, "first")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldBe a[ColumnErrors]
      val errors = resp1.swap.get.asInstanceOf[ColumnErrors].errs
      errors should have length 1
      errors.head shouldBe a[BadColumnParams]

      val resp2 = DataSchema.make("dataset", dataColSpecs :+ "h:hist:counter=bar", Nil, "first")
      resp2.isBad shouldEqual true
      resp2.swap.get shouldBe a[ColumnErrors]
      val errors2 = resp2.swap.get.asInstanceOf[ColumnErrors].errs
      errors2 should have length 1
      errors2.head shouldBe a[BadColumnParams]
    }

    it("should return BadColumnName if illegal chars in column name") {
      val resp1 = DataSchema.make("dataset", Seq("col, umn1:string"), Nil, "first")
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (1)
      errors.head shouldBe a[BadColumnName]
    }

    it("should return BadColumnType if unsupported type specified in column spec") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "part:linkedlist", Nil, "first")
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (1)
      errors.head shouldEqual BadColumnType("linkedlist")
    }

    it("should return BadColumnName if value column not one of other columns") {
      val conf2 = ConfigFactory.parseString("""
                    {
                      columns = ["first:string", "last:string", "age:long"]
                      value-column = "first2"
                      downsamplers = []
                    }""")
      val resp = DataSchema.fromConfig("dataset", conf2)
      resp.isBad shouldEqual true
      resp.swap.get shouldBe a[BadColumnName]
    }

    it("should return multiple column spec errors") {
      val resp1 = DataSchema.make("dataset", Seq("first:str", "age:long", "la(st):int"), Nil, "first")
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (2)
      errors.head shouldEqual BadColumnType("str")
    }

    it("should return BadDownsampler if no downsample-schema given when downsamplers present") {
      val conf2 = ConfigFactory.parseString("""
                    {
                      columns = ["timestamp:ts", "value:double:detectDrops=true"]
                      value-column = "value"
                      downsamplers = [ "tTime(0)", "dMin(1)", "dMax(1)", "dSum(1)", "dCount(1)", "dAvg(1)" ]
                    }""")
      val resp = DataSchema.fromConfig("dataset", conf2)
      resp.isBad shouldEqual true
      resp.swap.get shouldBe a[BadDownsampler]
    }

    it("should return NoTimestampRowKey if non timestamp used for row key / first column") {
      val ds1 = DataSchema.make("dataset", Seq("first:string", "age:long"), Nil, "first")
      ds1.isBad shouldEqual true
      ds1.swap.get shouldBe a[NoTimestampRowKey]
    }

    it("should return a valid Dataset when a good specification passed") {
      val conf2 = ConfigFactory.parseString("""
                    {
                      columns = ["timestamp:ts", "code:long", "event:string"]
                      value-column = "event"
                      downsamplers = []
                    }""")
      val schema = DataSchema.fromConfig("dataset", conf2).get
      schema.columns should have length (3)
      schema.columns.map(_.id) shouldEqual Seq(0, 1, 2)
      schema.columns.map(_.columnType) shouldEqual Seq(TimestampColumn, LongColumn, StringColumn)
      schema.timestampColumn.name shouldEqual "timestamp"
    }
  }

  val partSchemaStr = """{
                      columns = ["tags:map"]
                      predefined-keys = ["_ns", "app", "__name__", "instance", "dc"]
                      options {
                        copyTags = {}
                        ignoreShardKeyColumnSuffixes = {}
                        ignoreTagsOnPartitionKeyHash = ["le"]
                        metricColumn = "__name__"
                        shardKeyColumns = ["__name__", "_ns"]
                      }
                    }"""

  describe("PartitionSchema") {
    it("should allow MapColumns only in last position of partition key") {
      val mapCol = "tags:map"

      // OK: only partition column is map
      val ds1 = PartitionSchema.make(Seq(mapCol), DatasetOptions.DefaultOptions).get
      ds1.columns.map(_.name) should equal (Seq("tags"))

      // OK: last partition column is map
      val ds2 = PartitionSchema.make(Seq("first:string", mapCol), DatasetOptions.DefaultOptions).get
      ds2.columns.map(_.name) should equal (Seq("first", "tags"))

      // Not OK: first partition column is map
      val resp3 = PartitionSchema.make(Seq(mapCol, "first:string"), DatasetOptions.DefaultOptions)
      resp3.isBad shouldEqual true
      resp3.swap.get shouldBe an[IllegalMapColumn]
    }

    it("should return BadColumnType if unsupported type specified in column spec") {
      val resp1 = PartitionSchema.make(Seq("first:strolo"), DatasetOptions.DefaultOptions)
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (1)
      errors.head shouldEqual BadColumnType("strolo")
    }

    it("should parse config with options") {
      val conf2 = ConfigFactory.parseString(partSchemaStr)
      val schema = PartitionSchema.fromConfig(conf2).get

      schema.columns.map(_.columnType) shouldEqual Seq(MapColumn)
      schema.predefinedKeys shouldEqual Seq("_ns", "app", "__name__", "instance", "dc")
    }
  }

  describe("Schema") {
    it("should return IDs for column names or seq of missing names") {
      val sch = largeDataset.schema
      sch.colIDs("first", "age").get shouldEqual Seq(1, 0)

      sch.colIDs("league").get shouldEqual Seq(Dataset.PartColStartIndex)

      val resp1 = sch.colIDs("last", "unknown")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldEqual Seq("unknown")
    }

    it("should return ColumnInfos for colIDs") {
      val sch = largeDataset.schema
      val infos = sch.infosFromIDs(Seq(1, 0))
      infos shouldEqual Seq(ColumnInfo("first", StringColumn), ColumnInfo("age", LongColumn))

      val infos2 = sch.infosFromIDs(Seq(PartColStartIndex, 2))
      infos2 shouldEqual Seq(ColumnInfo("league", StringColumn), ColumnInfo("last", StringColumn))
    }
  }

  describe("Schemas") {
    it("should return all errors from every data schema") {
      val conf2 = ConfigFactory.parseString(s"""
                    {
                      partition-schema $partSchemaStr
                      schemas {
                        prom1 {
                          columns = ["timestamp:tsa", "code:long", "event:string"]
                          value-column = "event"
                          downsamplers = []
                        }
                        prom2 {
                          columns = ["timestamp:ts", "code:long", "ev. ent:string"]
                          value-column = "foo"
                          downsamplers = []
                        }
                        prom3 {
                          columns = ["timestamp:ts", "code:long", "event:string"]
                          value-column = "event"
                          downsamplers = []
                        }
                      }
                    }""")
      val resp = Schemas.fromConfig(conf2)
      resp.isBad shouldEqual true
      val errors = resp.swap.get
      errors should have length (2)
      errors.map(_._1).toSet shouldEqual Set("prom1", "prom2")
      errors.map(_._2.getClass).toSet shouldEqual Set(classOf[ColumnErrors])
    }

    it("should detect and report hash conflicts") {
      val conf2 = ConfigFactory.parseString(s"""
                    {
                      partition-schema $partSchemaStr
                      schemas {
                        prom {
                          columns = ["timestamp:ts", "value:double"]
                          value-column = "value"
                          downsamplers = []
                        }
                        prom2 {
                          columns = ["timestamp:ts", "value:double"]
                          value-column = "timestamp"
                          downsamplers = []
                        }
                      }
                    }""")
      val resp = Schemas.fromConfig(conf2)
      resp.isBad shouldEqual true
      val errors = resp.swap.get
      errors.map(_._2.getClass) shouldEqual Seq(classOf[HashConflict])
    }

    it("should detect and report invalid downsample-schema references") {
      val conf2 = ConfigFactory.parseString(s"""
                    {
                      partition-schema $partSchemaStr
                      schemas {
                        prom {
                          columns = ["timestamp:ts", "value:double"]
                          value-column = "value"
                          downsamplers = ["tTime(0)", "dMin(1)"]
                          downsample-schema = "foo"
                        }
                        prom-ds-gauge {
                          columns = ["timestamp:ts", "min:double"]
                          value-column = "timestamp"
                          downsamplers = []
                        }
                      }
                    }""")
      val resp = Schemas.fromConfig(conf2)
      resp.isBad shouldEqual true
      val errors = resp.swap.get
      errors.map(_._2.getClass) shouldEqual Seq(classOf[BadDownsampler])
      errors.map(_._1) shouldEqual Seq("prom")
    }

    def schemasFromString(partConf: String) = ConfigFactory.parseString(s"""
                  {
                    partition-schema $partConf
                    schemas {
                      prom {
                        columns = ["timestamp:ts", "value:double"]
                        value-column = "value"
                        downsamplers = ["tTime(0)", "dMin(1)"]
                        downsample-schema = "prom-ds-gauge"
                      }
                      prom-ds-gauge {
                        columns = ["timestamp:ts", "min:double"]
                        value-column = "timestamp"
                        downsamplers = []
                      }
                      hist {
                        columns = ["timestamp:ts", "count:long", "sum:long", "h:hist:counter=true"]
                        value-column = "h"
                        downsamplers = []
                      }
                    }
                  }""")

    it("should return Schemas instance with every schema parsed") {
      val conf2 = schemasFromString(partSchemaStr)
      val schemas = Schemas.fromConfig(conf2).get

      schemas.part.columns.map(_.columnType) shouldEqual Seq(MapColumn)
      schemas.part.columns.map(_.id) shouldEqual Seq(PartColStartIndex)
      schemas.part.predefinedKeys shouldEqual Seq("_ns", "app", "__name__", "instance", "dc")
      Dataset.isPartitionID(schemas.part.columns.head.id) shouldEqual true

      schemas.schemas.keySet shouldEqual Set("prom", "hist", "prom-ds-gauge")
      schemas.schemas("prom").data.columns.map(_.columnType) shouldEqual Seq(TimestampColumn, DoubleColumn)
      schemas.schemas("prom").data.columns.map(_.id) shouldEqual Seq(0, 1)
      schemas.schemas("prom").data.timestampColumn.name shouldEqual "timestamp"
      schemas.schemas("hist").data.columns.map(_.columnType) shouldEqual
        Seq(TimestampColumn, LongColumn, LongColumn, HistogramColumn)
      schemas.schemas("prom").downsample.get shouldEqual schemas.schemas("prom-ds-gauge")
    }

    val partSchemaStr2 = """{
                        columns = ["metric:string", "tags:map"]
                        predefined-keys = ["_ns", "app", "__name__", "instance", "dc"]
                        options {
                          copyTags = {}
                          ignoreShardKeyColumnSuffixes = {}
                          ignoreTagsOnPartitionKeyHash = ["le"]
                          metricColumn = "metric"
                          shardKeyColumns = ["metric", "_ns"]
                        }
                      }"""

    it("should return unique schema hashes when partition keys different") {
      val conf1 = schemasFromString(partSchemaStr)
      val conf2 = schemasFromString(partSchemaStr2)
      val schemas1 = Schemas.fromConfig(conf1).get
      val schemas2 = Schemas.fromConfig(conf2).get

      schemas1.schemas("prom").schemaHash should not equal (schemas2.schemas("prom").schemaHash)
      schemas1.schemas("hist").schemaHash should not equal (schemas2.schemas("hist").schemaHash)
    }

    it("should allow column type params to differentiate hash") {
        val conf3 = ConfigFactory.parseString(s"""
                      {
                        partition-schema $partSchemaStr
                        schemas {
                          prom {
                            columns = ["timestamp:ts", "value:double"]
                            value-column = "value"
                            downsamplers = ["tTime(0)", "dMin(1)"]
                            downsample-schema = "prom2"
                          }
                          # Everything exactly the same except for column params, which are different
                          prom2 {
                            columns = ["timestamp:ts", "value:double:detectDrops=true"]
                            value-column = "timestamp"
                            downsamplers = []
                          }
                        }
                      }""")
        val schemas = Schemas.fromConfig(conf3).get

        schemas.schemas.keySet shouldEqual Set("prom", "prom2")
        schemas.schemas("prom").data.columns.map(_.columnType) shouldEqual Seq(TimestampColumn, DoubleColumn)
        schemas.schemas("prom").data.columns.map(_.id) shouldEqual Seq(0, 1)
        schemas.schemas("prom").data.timestampColumn.name shouldEqual "timestamp"
        schemas.schemas("prom").downsample.get shouldEqual schemas.schemas("prom2")
    }
  }
}
