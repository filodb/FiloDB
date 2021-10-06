package filodb.core.memstore

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spire.syntax.cfor.cforRange

class EvictablePartIdQueueSetSpec  extends AnyFunSpec with Matchers  {

  it("should remove items in same order as added") {

    val q = new EvictablePartIdQueueSet(100)
    cforRange ( 0 until 8 ) { i =>
      q.put(i)
    }
    val sink = debox.Buffer.ofSize[Int](10)
    q.removeInto(10, sink)
    sink.toList() shouldEqual (0 until 8)

  }

  it("should not bother adding duplicate items") {
    val q = new EvictablePartIdQueueSet(100)
    cforRange ( 0 until 8 ) { i =>
      q.put(i)
      q.put(i)
    }
    val sink = debox.Buffer.ofSize[Int](10)
    q.removeInto(10, sink)
    sink.toList() shouldEqual (0 until 8)
  }

  it("should resize as needed") {
    val q = new EvictablePartIdQueueSet(4)
    q.size shouldEqual 0
    q.arr.items.length shouldEqual 8 // minimum initial size of array is 8
    cforRange ( 0 until 16 ) { i =>
      q.put(i)
      q.put(i)
    }
    q.size shouldEqual 16
    q.arr.items.length shouldEqual 32 // size of re-sized array
    val sink = debox.Buffer.ofSize[Int](20)
    q.removeInto(20, sink)
    sink.toList() shouldEqual (0 until 16)
    q.size shouldEqual 0
    q.arr.items.length shouldEqual 32 // size of array does not reduce
  }

}
