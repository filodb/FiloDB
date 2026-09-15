package filodb.coordinator.flight

import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}
import io.grpc._

object ZstdCompressor extends Compressor {
  override def getMessageEncoding: String = "zstd"
  override def compress(os: java.io.OutputStream): java.io.OutputStream = new ZstdOutputStream(os)
}

object ZstdDecompressor extends Decompressor {
  override def getMessageEncoding: String = "zstd"
  override def decompress(is: java.io.InputStream): java.io.InputStream = new ZstdInputStream(is)
}

object ZstdCodecs {
  val compressorRegistry: CompressorRegistry = {
    val r = CompressorRegistry.newEmptyInstance()
    r.register(ZstdCompressor)
    r
  }
  val decompressorRegistry: DecompressorRegistry =
    DecompressorRegistry.getDefaultInstance.`with`(ZstdDecompressor, true)
}

object ZstdServerInterceptor extends ServerInterceptor {

  private val AcceptEncodingKey =
    Metadata.Key.of("grpc-accept-encoding", Metadata.ASCII_STRING_MARSHALLER)

  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT],
                                          headers: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val acceptEncoding = headers.get(AcceptEncodingKey)
    if (acceptEncoding != null && acceptEncoding.split(",").map(_.trim).contains("zstd")) {
      call.setCompression("zstd")
    }
    next.startCall(call, headers)
  }
}

object ZstdClientInterceptor extends ClientInterceptor {
  private val AcceptEncodingKey =
    Metadata.Key.of("grpc-accept-encoding", Metadata.ASCII_STRING_MARSHALLER)

  override def interceptCall[ReqT, RespT](method: MethodDescriptor[ReqT, RespT],
                                          callOptions: CallOptions,
                                          next: Channel): ClientCall[ReqT, RespT] = {
    val delegate = next.newCall(method, callOptions.withCompression("zstd"))
    new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](delegate) {
      override def start(responseListener: ClientCall.Listener[RespT], headers: Metadata): Unit = {
        headers.put(AcceptEncodingKey, "zstd")
        super.start(responseListener, headers)
      }
    }
  }
}
