package filodb.memory.format

import java.nio.ByteBuffer

import com.kenai.jffi.MemoryIO
import org.agrona.DirectBuffer
import spire.implicits.cforRange
// scalastyle:off number.of.methods
object UnsafeUtils {
  // scalastyle:off
  // Copy and translation from: https://github.com/scala/scala/blob/2.13.x/src/library/scala/runtime/Statics.java#L1760
  final val unsafe: sun.misc.Unsafe = classOf[sun.misc.Unsafe].getDeclaredFields
    .find(_.getType == classOf[sun.misc.Unsafe])
    .map { field => {
      field.setAccessible(true)
      field.get(null).asInstanceOf[sun.misc.Unsafe]
    }
    } getOrElse (throw new IllegalStateException("Can't find instance of sun.misc.Unsafe"))
  // scalastyle:on

  // scalastyle:off
  val ZeroPointer: Any = null
  val ZeroArray = null.asInstanceOf[Array[Byte]]
  // scalastyle:on

  val arayOffset = unsafe.arrayBaseOffset(classOf[Array[Byte]])

  // These are specific to JDK8, definitely not guaranteed for other versions
  val byteBufArrayField = unsafe.objectFieldOffset(classOf[ByteBuffer].getDeclaredField("hb"))
  val byteBufOffsetField = unsafe.objectFieldOffset(classOf[ByteBuffer].getDeclaredField("offset"))

  /** Translate ByteBuffer into base, offset, numBytes */
  //scalastyle:off method.name
  def BOLfromBuffer(buf: ByteBuffer): (Any, Long, Int) = {
    if (buf.hasArray) {
      (buf.array, arayOffset.toLong + buf.arrayOffset + buf.position(), buf.limit() - buf.position())
    } else if (buf.isDirect) {
      val address = MemoryIO.getCheckedInstance.getDirectBufferAddress(buf)
      (UnsafeUtils.ZeroPointer, address + buf.position(), buf.limit() - buf.position())
    } else {
      assert(buf.isReadOnly)
      (unsafe.getObject(buf, byteBufArrayField).asInstanceOf[Array[Byte]],
       unsafe.getInt(buf, byteBufOffsetField) + arayOffset,
       buf.limit() - buf.position())
    }
  }
  //scalastyle:on method.name

  def addressFromDirectBuffer(buf: ByteBuffer): Long = {
    assert(buf.isDirect)
    MemoryIO.getCheckedInstance.getDirectBufferAddress(buf) + buf.position()
  }

  def asDirectBuffer(address: Long, size: Int): ByteBuffer = {
    MemoryIO.getCheckedInstance.newDirectByteBuffer(address, size)
  }

  def wrapDirectBuf(base: Any, offset: Long, numBytes: Int, buf: DirectBuffer): Unit = {
    if (base != UnsafeUtils.ZeroPointer) {
      buf.wrap(base.asInstanceOf[Array[Byte]], offset.toInt - arayOffset, numBytes)
    } else {
      buf.wrap(offset, numBytes)
    }
  }

  def printBytes(buf: DirectBuffer, numBytes: Int = 16): Unit = {
    //scalastyle:off
    println((0 until numBytes).map(buf.getByte).map(i => f"$i%02x").mkString(" "))
    //scalastyle:on
  }

  /**
   * Generic methods to read and write data to any offset from a base object location.  Be careful, this
   * can easily crash the system!
   */
  final def getByte(obj: Any, offset: Long): Byte = unsafe.getByte(obj, offset)
  final def getShort(obj: Any, offset: Long): Short = unsafe.getShort(obj, offset)
  final def getInt(obj: Any, offset: Long): Int = unsafe.getInt(obj, offset)
  final def getIntVolatile(obj: Any, offset: Long): Int = unsafe.getIntVolatile(obj, offset)
  final def getLong(obj: Any, offset: Long): Long = unsafe.getLong(obj, offset)
  final def getLongVolatile(obj: Any, offset: Long): Long = unsafe.getLongVolatile(obj, offset)
  final def getDouble(obj: Any, offset: Long): Double = unsafe.getDouble(obj, offset)
  final def getFloat(obj: Any, offset: Long): Double = unsafe.getFloat(obj, offset)

  final def getByte(addr: Long): Byte = unsafe.getByte(ZeroPointer, addr)
  final def getShort(addr: Long): Short = unsafe.getShort(ZeroPointer, addr)
  final def getInt(addr: Long): Int = unsafe.getInt(ZeroPointer, addr)
  final def getLong(addr: Long): Long = unsafe.getLong(ZeroPointer, addr)
  final def getLongVolatile(addr: Long): Long = unsafe.getLongVolatile(ZeroPointer, addr)
  final def getDouble(addr: Long): Double = unsafe.getDouble(ZeroPointer, addr)
  final def getFloat(addr: Long): Double = unsafe.getFloat(ZeroPointer, addr)

  final def setByte(obj: Any, offset: Long, byt: Byte): Unit = unsafe.putByte(obj, offset, byt)
  final def setShort(obj: Any, offset: Long, s: Short): Unit = unsafe.putShort(obj, offset, s)
  final def setInt(obj: Any, offset: Long, i: Int): Unit = unsafe.putInt(obj, offset, i)
  final def setIntVolatile(obj: Any, offset: Long, i: Int): Unit = unsafe.putIntVolatile(obj, offset, i)
  final def setLong(obj: Any, offset: Long, l: Long): Unit = unsafe.putLong(obj, offset, l)
  final def setDouble(obj: Any, offset: Long, d: Double): Unit = unsafe.putDouble(obj, offset, d)
  final def setFloat(obj: Any, offset: Long, f: Float): Unit = unsafe.putFloat(obj, offset, f)

  final def setByte(addr: Long, byt: Byte): Unit = unsafe.putByte(ZeroPointer, addr, byt)
  final def setShort(addr: Long, s: Short): Unit = unsafe.putShort(ZeroPointer, addr, s)
  final def setInt(addr: Long, i: Int): Unit = unsafe.putInt(ZeroPointer, addr, i)
  final def setLong(addr: Long, l: Long): Unit = unsafe.putLong(ZeroPointer, addr, l)
  final def setDouble(addr: Long, d: Double): Unit = unsafe.putDouble(ZeroPointer, addr, d)

  def copy(source: Long, dest: Long, numBytes: Int): Unit =
    unsafe.copyMemory(ZeroPointer, source, ZeroPointer, dest, numBytes)

  def copy(src: Any, srcOffset: Long, dest: Any, destOffset: Long, numBytes: Long): Unit =
    unsafe.copyMemory(src, srcOffset, dest, destOffset, numBytes)

  /**
   * Matches two memory buffers of length numBytes, returns true if they are byte for byte equal
   * Compares long words for speed
   */
  def equate(srcObj: Any, srcOffset: Long, destObj: Any, destOffset: Long, numBytes: Int): Boolean = {
    var i = 0
    while (i <= numBytes - 8) {
      if (getLong(srcObj, srcOffset + i) != getLong(destObj, destOffset + i)) return false
      i += 8
    }
    while (i < numBytes) {
      if (getByte(srcObj, srcOffset + i) != getByte(destObj, destOffset + i)) return false
      i += 1
    }
    true
  }


  // Comparison of two memories assuming both are word aligned and length is rounded to next word (4 bytes)
  // Also assumes a little-endian (eg Intel) architecture
  // Returns >0 if src is greater than dest, 0 if equal, <0 if src less than dest.  Can be used for sorts.
  def wordCompare(srcObj: Any, srcOffset: Long, destObj: Any, destOffset: Long, n: Int): Int = {
    import java.lang.Integer.reverseBytes
    var i = 0
    while (i < n) {
      val srcWord = reverseBytes(getInt(srcObj, srcOffset + i)) ^ 0x80000000
      val destWord = reverseBytes(getInt(destObj, destOffset + i)) ^ 0x80000000
      if (srcWord < destWord) return -1 else if (srcWord != destWord) return 1
      i += 4
    }
    0
  }

  /**
   * Compares two memories of arbitrary length, lexicographically, returning <0, 0, or >0
   */
  def compare(base1: Any, offset1: Long, numBytes1: Int, base2: Any, offset2: Long, numBytes2: Int): Int = {
    val minLen = Math.min(numBytes1, numBytes2)
    val minLenAligned = minLen & -4
    val wordComp = wordCompare(base1, offset1, base2, offset2, minLenAligned)
    if (wordComp == 0) {
      var pointer1 = offset1 + minLenAligned
      var pointer2 = offset2 + minLenAligned
      cforRange { minLenAligned until minLen } { _ =>
        val res = (getByte(base1, pointer1) & 0xff) - (getByte(base2, pointer2) & 0xff)
        if (res != 0) return res
        pointer1 += 1
        pointer2 += 1
      }
      return numBytes1 - numBytes2
    } else wordComp
  }
}
