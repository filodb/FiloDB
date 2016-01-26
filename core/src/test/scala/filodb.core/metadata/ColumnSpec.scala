package filodb.core.metadata

import org.scalatest.FunSpec
import org.scalatest.Matchers

class ColumnSpec extends FunSpec with Matchers {
  import Column.ColumnType

  val firstColumn = DataColumn(0, "first", "foo", 1, ColumnType.StringColumn)
  val ageColumn = DataColumn(2, "age", "foo", 1, ColumnType.IntColumn)
  val schema = Map("first" -> firstColumn, "age" -> ageColumn)

  describe("Column.schemaFold") {
    it("should add new columns to the schema") {
      val deletedCol = DataColumn(3, ":deleted", "foo", 1, ColumnType.BitmapColumn)
      Column.schemaFold(schema, deletedCol) should equal (schema + (":deleted" -> deletedCol))
    }

    it("should remove deleted columns from the schema") {
      val deletedCol = firstColumn.copy(version = 2, isDeleted = true)
      Column.schemaFold(schema, deletedCol) should equal (schema - "first")
    }

    it("should replace updated column defs in the schema") {
      val newCol = ageColumn.copy(version = 3, columnType = ColumnType.StringColumn)
      Column.schemaFold(schema, newCol) should equal (Map("first" -> firstColumn, "age" -> newCol))
    }
  }

  describe("Column.invalidateNewColumn") {
    it("should check that regular column names don't have : in front") {
      val newCol = ageColumn.copy(name = ":illegal")
      val reasons = Column.invalidateNewColumn("foo", schema, newCol)
      reasons should have length 1
      reasons.head should startWith ("Illegal char :")
    }

    it("should check that column names cannot contain illegal chars") {
      def checkIsIllegal(name: String): Unit = {
        val newCol = ageColumn.copy(name = name)
        val reasons = Column.invalidateNewColumn("foo", schema, newCol)
        reasons.head should startWith ("Illegal char")
      }

      checkIsIllegal("ille gal")
      checkIsIllegal("(illegal)")
      checkIsIllegal("ille\001gal")
    }

    it("should check that cannot add columns at lower versions") {
      val newCol = ageColumn.copy(version = 0, columnType = ColumnType.StringColumn)
      val reasons = Column.invalidateNewColumn("foo", schema, newCol)
      reasons should have length 1
      reasons.head should include ("at version lower")
    }

    it("should check that added columns change some property") {
      val newCol = ageColumn.copy(version = 2)
      val reasons = Column.invalidateNewColumn("foo", schema, newCol)
      reasons should have length 1
      reasons.head should startWith ("Nothing changed")
    }

    it("should check that new columns are not deleted") {
      val deletedCol = firstColumn.copy(name = "last", isDeleted = true)
      val reasons = Column.invalidateNewColumn("foo", schema, deletedCol)
      reasons should have length 1
      reasons.head should equal ("New column cannot be deleted")
    }

    it("should return no reasons for a valid new column") {
      val newCol = ageColumn.copy(version = 4, columnType = ColumnType.StringColumn)
      val reasons = Column.invalidateNewColumn("foo", schema, newCol)
      reasons should have length 0
    }
  }

  describe("Column serialization") {
    // See https://github.com/apache/spark/pull/7122 - serialization bug involving Class[Long] etc.
    // TODO(velvia): Don't rely on standard Java serialization of columns.  Instead replace with toString etc.
    ignore("should serialize and deserialize properly") {
      val baos = new java.io.ByteArrayOutputStream
      val oos = new java.io.ObjectOutputStream(baos)
      oos.writeObject(ageColumn)

      val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
      val ois = new java.io.ObjectInputStream(bais)
      val readColumn = ois.readObject().asInstanceOf[Column]

      readColumn should equal (ageColumn)
    }
  }
}