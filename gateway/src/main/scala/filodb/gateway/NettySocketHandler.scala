package filodb.gateway

import com.typesafe.scalalogging.StrictLogging
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import filodb.memory.format.UnsafeUtils

/**
 * Handler implementation for TCP metrics socket reader
 *
 * @param delimiter an optional byte to "split" incoming messages by.  Typically \n for string formatted content
 *                  NOTE: for string data you really need to use this because TCP buffers truncate data at
 *                  arbitrary points so you are not guaranteed to get the entire message in one go.
 * @param handler a callback to handle individual messages
 */
class NettySocketHandler(delimiter: Option[Byte],
                         handler: ChannelBuffer => Unit) extends SimpleChannelUpstreamHandler with StrictLogging {
  // We split the message using delimiter into separate byte arrays.
  // YES this copies bytes, but the use case is that we need to put those bytes into various queues and they
  // cannot be freed right away.
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val buffer = e.getMessage.asInstanceOf[ChannelBuffer]
    val numBytes = buffer.readableBytes
    var lastFragment = Option(ctx.getAttachment.asInstanceOf[ChannelBuffer])

    delimiter match {
      case Some(delimByte) =>
        var curIndex = buffer.readerIndex
        val destIndex = curIndex + numBytes
        while (curIndex < destIndex) {
          // search for delimiter
          val delimitIndex = buffer.indexOf(curIndex, destIndex, delimByte)
          if (delimitIndex >= 0) {
            val buf = lastFragment.map { frag =>
                        buffer.readBytes(frag, delimitIndex - curIndex)
                        lastFragment = None
                        ctx.setAttachment(UnsafeUtils.ZeroPointer)  // don't leave fragment for next buffer
                        frag
                      }.getOrElse(buffer.readSlice(delimitIndex - curIndex))
            handler(buf)
            if (delimitIndex < destIndex) buffer.skipBytes(1)  // skip over delimiter
            curIndex = delimitIndex + 1
          } else {
            val fragBuf = ChannelBuffers.dynamicBuffer(512)
            buffer.readBytes(fragBuf, destIndex - curIndex)
            // Read remaining fragment into a ChannelBuffer and stash it in the context for the next frame
            ctx.setAttachment(fragBuf)
            logger.debug(s"Storing fragment with ${destIndex - curIndex} bytes for next buffer")
            curIndex = destIndex
          }
        }
      case None =>
        handler(buffer.readSlice(numBytes))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    // Close the connection when an exception is raised.
    e.getCause().printStackTrace()
    e.getChannel().close()
  }
}
