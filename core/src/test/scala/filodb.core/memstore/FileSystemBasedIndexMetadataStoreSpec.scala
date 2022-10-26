package filodb.core.memstore

import filodb.core.GdeltTestData.{datasetOptions, schema}
import filodb.core.metadata.Dataset
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.{FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import scala.util.{Failure, Try}

// scalastyle:off
class FileSystemBasedIndexMetadataStoreSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {


  val snap = 1663092567977L

  val dataset = Dataset("gdelt", schema.slice(4, 6), schema.patch(4, Nil, 2), datasetOptions)

  it("initState with no gen file and expected version passed should Trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  override def beforeEach(): Unit  = {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val buffer: ByteBuffer = ByteBuffer.allocate(8)
    buffer.putLong(snap)
    val fos = new FileOutputStream(snapFile)
    fos.write(FileSystemBasedIndexMetadataStore.snapFileV1Magic)
    fos.write(buffer.array())
    fos.close()
  }


  it("initState with expectedVersion with no magic bytes in gen file should Trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.createNewFile()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  it("initState with expectedVersion with invalid magic bytes in gen file should Trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    os.write(0)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  it("initState with expectedVersion with valid magic bytes and less than 4 bytes for version should trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    os.write(FileSystemBasedIndexMetadataStore.genFileV1Magic)
    os.write(Array[Byte](0, 1, 2)) // Length as 3 bytes instead of expected 4
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  it("initState with expectedVersion, valid magic bytes and 4 bytes for version should invoke currentState") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    os.write(FileSystemBasedIndexMetadataStore.genFileV1Magic)
    os.write(Array[Byte](0, 0, 0, 1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snap)))
  }

  it("initState with expectedVersion < version in gen file should invoke currentState") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    os.write(FileSystemBasedIndexMetadataStore.genFileV1Magic)
    os.write(Array[Byte](0, 0, 0, 1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snap)))
  }


  it("initState with expectedVersion = version in gen file should invoke currentState") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    os.write(FileSystemBasedIndexMetadataStore.genFileV1Magic)
    os.write(Array[Byte](0, 0, 0, 1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snap)))
  }

  it("initState with expectedVersion > version in gen file should trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(2))
    os.write(FileSystemBasedIndexMetadataStore.genFileV1Magic)
    os.write(Array[Byte](0, 0, 0, 1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  it("initState with no expected version should invoke currentState") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    genFile.delete()
    val os = new FileOutputStream(genFile)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    os.write(FileSystemBasedIndexMetadataStore.genFileV1Magic)
    os.write(Array[Byte](0, 0, 0, 1))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snap)))
  }


  it("currentState with no snap file should be Empty") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    snapFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
  }


  it("currentState with invalid magic byte should  should Trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val fos = new FileOutputStream(snapFile)
    fos.write(0)
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    // invalid magic bytes should trigger rebuild, but currentState should throw exception
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    Try(store.currentState(dataset.ref, 0)) match {
      case Failure(e: IllegalStateException)  => e.getMessage shouldEqual "Invalid magic bytes in snap file"
      case _                                  => fail("Expected to see Invalid magic bytes")
    }
  }

  it("currentState with truncated length should Trigger rebuild") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    snapFile.delete()
    val fos = new FileOutputStream(snapFile)
    fos.write(FileSystemBasedIndexMetadataStore.snapFileV1Magic)
    fos.write(Array[Byte](0, 0, 0))
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    Try(store.currentState(dataset.ref, 0)) match {
      case Failure(e: IllegalStateException)  => e.getMessage shouldEqual "Snap file truncated"
      case _                                  => fail("Expected to see Invalid magic bytes")
    }
  }

  it("currentState with valid snap file should return Synced with correct timestamp") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snap)))
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snap)))
  }

  it("should update snap file but not gen file when updateState with Synced state is invoked with expectedGeneration as None") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    val snapTime = System.currentTimeMillis()
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual false
    val snapIn = new FileInputStream(snapFile)
    snapIn.read() shouldEqual FileSystemBasedIndexMetadataStore.snapFileV1Magic
    val bytes = Array[Byte](0, 0, 0, 0 ,0, 0, 0, 0)
    snapIn.read(bytes) shouldEqual 8
    ByteBuffer.wrap(bytes).getLong shouldEqual snapTime
  }

  it("should update snap file and gen file when updateState with Synced state is invoked with expectedGeneration as Some value") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(10))
    val snapTime = System.currentTimeMillis()
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual true
    val snapIn = new FileInputStream(snapFile)
    snapIn.read() shouldEqual FileSystemBasedIndexMetadataStore.snapFileV1Magic
    val bytes = Array[Byte](0, 0, 0, 0 ,0, 0, 0, 0)
    snapIn.read(bytes) shouldEqual 8
    ByteBuffer.wrap(bytes).getLong shouldEqual snapTime

    val genIn = new FileInputStream(genFile)
    genIn.read() shouldEqual FileSystemBasedIndexMetadataStore.genFileV1Magic
    val genBytes = Array[Byte](0, 0, 0, 0)
    genIn.read(genBytes) shouldEqual 4
    ByteBuffer.wrap(genBytes).getInt shouldEqual 10
  }

  it("should not Trigger rebuild where state is synced and expected version is written to gen file") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(10))
    val snapTime = System.currentTimeMillis()
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual true
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
  }


  it("should not Trigger rebuild where state is synced and expected version is None") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    val snapTime = System.currentTimeMillis()
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual false
    // As long as snap file exists, existence of gen file does not matter
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
  }

  it("initState should return TriggerRebuild when state is updated to TriggerRebuild, the currentState should not be affected") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    val snapTime = System.currentTimeMillis()
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual true
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    // Now update state to TriggerRebuild and the return value should be TriggerRebuild, if snap file exists it
    // should be unaffected as its still needed for currently running shard to update its state
    store.updateState(dataset.ref, 0, IndexState.TriggerRebuild, snapTime)
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual false
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
  }

  it("initState should return TriggerRebuild when state is updated to Empty and expectedVersion is not None, the currentState should be Empty") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, Some(1))
    val snapTime = System.currentTimeMillis()
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual true
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    // Now update state to TriggerRebuild and the return value should be TriggerRebuild, if snap file exists it
    // should be unaffected as its still needed for currently running shard to update its state
    store.updateState(dataset.ref, 0, IndexState.Empty, snapTime)
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    snapFile.exists() shouldEqual false
    genFile.exists() shouldEqual false
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
  }

  it("initState should return Empty when state is updated to Empty and expectedVersion is None, the currentState should be Empty") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None)
    val snapTime = System.currentTimeMillis()
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual false
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    // Now update state to TriggerRebuild and the return value should be TriggerRebuild, if snap file exists it
    // should be unaffected as its still needed for currently running shard to update its state
    store.updateState(dataset.ref, 0, IndexState.Empty, snapTime)
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
    snapFile.exists() shouldEqual false
    genFile.exists() shouldEqual false
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
  }

  it("initState should TriggerRebuild if state is older than maxRefreshHours") {
    val tmpFile = System.getProperty("java.io.tmpdir")
    val snapFile = new java.io.File(tmpFile, "_gdelt_0_snap")
    val genFile = new java.io.File(tmpFile, "_gdelt_0_rebuild.gen")
    snapFile.delete()
    genFile.delete()
    val store = new FileSystemBasedIndexMetadataStore(tmpFile, None, 2)

    val snapTime = System.currentTimeMillis() - 4 * 60 * 60 * 1000 // make snapTime 4 hours old
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None)) // two hours of max sync time
    store.updateState(dataset.ref, 0, IndexState.Synced, snapTime)
    snapFile.exists() shouldEqual true
    genFile.exists() shouldEqual false
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Synced, Some(snapTime)))
    // IMPORTANT: Current state is still Synced. If shards are running, it makes no sense to TriggerRebuild
    // as it happens only on start up. Perhaps in future iterations if the refresh overload is high, shard can
    // gracefully shutdown to rebuild the index from scratch
  }

  it("should parse the expectedVersion when a numeric value as String is provided") {
    FileSystemBasedIndexMetadataStore.expectedVersion("123") shouldEqual Some(123)
  }

  it("should return None if null string is provided") {
    FileSystemBasedIndexMetadataStore.expectedVersion(null) shouldEqual None
  }

  it("should return None if non numeric string is provided") {
    FileSystemBasedIndexMetadataStore.expectedVersion("test") shouldEqual None
  }

  it("should return None if overflowing numeric string is provided") {
    FileSystemBasedIndexMetadataStore.expectedVersion("12345678686774553466 ") shouldEqual None
  }
}
// scalastyle:on
