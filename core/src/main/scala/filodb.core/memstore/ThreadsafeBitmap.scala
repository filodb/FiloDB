package filodb.core.memstore

import java.util.concurrent.locks.StampedLock

import com.googlecode.javaewah.EWAHCompressedBitmap

/**
  * Decorates a EWAHCompressedBitmap so that it is thread-safe.
  * It uses StampedLock for high throughput
  */
class ThreadsafeBitmap(bitmap: EWAHCompressedBitmap) {

  private final val lock = new StampedLock()

  def get(partID: Integer): Boolean = {
    var stamp = lock.tryOptimisticRead()
    var ingesting = bitmap.get(partID)
    if (!lock.validate(stamp)) {
      stamp = lock.readLock()
      try {
        ingesting = bitmap.get(partID)
      } finally {
        lock.unlockRead(stamp)
      }
    }
    ingesting
  }

  def set(partID: Int): Boolean = {
    val stamp = lock.writeLock()
    try {
      bitmap.set(partID)
    } finally {
      lock.unlockWrite(stamp)
    }
  }

  def clear(partID: Int): Boolean = {
    val stamp = lock.writeLock()
    try {
      bitmap.clear(partID)
    } finally {
      lock.unlockWrite(stamp)
    }
  }

  def cardinality(): Int = {
    var stamp = lock.tryOptimisticRead()
    var card = bitmap.cardinality()
    if (!lock.validate(stamp)) {
      stamp = lock.readLock()
      try {
        card = bitmap.cardinality()
      } finally {
        lock.unlockRead(stamp)
      }
    }
    card
  }

  def and(other: EWAHCompressedBitmap): EWAHCompressedBitmap = {
    var stamp = lock.tryOptimisticRead()
    var result = bitmap.and(other)
    if (!lock.validate(stamp)) {
      stamp = lock.readLock()
      try {
        result = bitmap.and(other)
      } finally {
        lock.unlockRead(stamp)
      }
    }
    result
  }
}
