package filodb.akkabootstrapper

import scalaj.http.Http

final class ConsulClient(val settings: AkkaBootstrapperSettings) {

  val consulApiHost: String = settings.consulApiHost
  val consulApiPort: Int = settings.consulApiPort

  def register(serviceId: String, registrationServiceName: String, host: String, port: Int): Unit = {
    val registrationPayload =
      s"""{"id": "$serviceId", "name": "$registrationServiceName", "address": "$host", "port": $port}"""
    val response = Http(
      s"http://$consulApiHost:$consulApiPort/v1/agent/service/register").put(registrationPayload).asString
    if (!response.is2xx) {
      throw new IllegalStateException(
        s"Service registration not successful. Got response code ${response.code} with body ${response.body}")
    }
  }

  def deregister(serviceId: String): Unit = {
    val response = Http(s"http://$consulApiHost:$consulApiPort/v1/agent/service/deregister/$serviceId").put("").asString
    if (!response.is2xx) {
      throw new IllegalStateException(
        s"Service de-registration not successful. Got response code ${response.code} with body ${response.body}")
    }

  }
}
