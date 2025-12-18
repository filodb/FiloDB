package filodb.coordinator.flight

import java.util.Collections

import com.typesafe.config.Config
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator

import filodb.core.memstore.TimeSeriesStore
import filodb.core.query._
import filodb.query.exec.ExecPlan

/**
 * FiloDB Flight Producer - serves Flight RPCs for FiloDB
 * It extends FlightQueryExecutor to execute queries
 */
class FiloDBFlightProducer(val memStore: TimeSeriesStore,
                           val allocator: BufferAllocator,
                           val location: Location,
                           val sysConfig: Config) extends NoOpFlightProducer with FlightQueryExecutor {

  override def listActions(context: FlightProducer.CallContext,
                           listener: FlightProducer.StreamListener[ActionType]): Unit = {
    // empty for now since this is only for reads, no updates or actions supported
    listener.onCompleted()
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria,
                           listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    // empty for now - we dont support listing flights since we only support Command FlightDescriptors
    listener.onCompleted()
  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    if (!descriptor.isCommand) {
      throw new UnsupportedOperationException("Only Command FlightDescriptors are supported")
    } else {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getCommand), location)
      new FlightInfo(ArrowSerializedRangeVector.arrowSrvSchema,
        descriptor, Collections.singletonList(flightEndpoint), -1, -1)
    }
  }

  /**
   * Handle doGet requests - execute query plan and stream results
   */
  // scalastyle:off method.length
  override def getStream(context: FlightProducer.CallContext,
                         ticket: Ticket,
                         listener: ServerStreamListener): Unit = {

    // scalastyle:off null
    try {
      FlightKryoSerDeser.deserialize(ticket.getBytes) match {
        case execPlan: ExecPlan =>
          executePhysicalPlanEntry(context, execPlan, listener)
        case other =>
          val errMsg = s"Invalid ticket type ${other.getClass.getName}, expected ExecPlan"
          logger.error(errMsg)
          listener.error(new IllegalArgumentException(errMsg))
      }
    } catch {
      case ex: Throwable =>
        logger.error("Error executing plan", ex)
        listener.error(ex)
    }
  }

}
