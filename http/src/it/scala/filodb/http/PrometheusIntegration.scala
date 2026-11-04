package filodb.http

/**
 * ScalaTest tag for the live Prometheus-comparison integration test.
 *
 * The harness lives in the sbt IntegrationTest config (`http/src/it`), so it is not part of a
 * normal `sbt http/test` run. The tagged test also self-cancels (via `assume`) when no Prometheus
 * is reachable. Run the whole harness with `sbt http/it:test`, or select this test alone with
 * `sbt "http/it:testOnly filodb.http.promcompat.PromCompatSpec -- -n filodb.http.PrometheusIntegration"`.
 */
object PrometheusIntegration extends org.scalatest.Tag("filodb.http.PrometheusIntegration")
