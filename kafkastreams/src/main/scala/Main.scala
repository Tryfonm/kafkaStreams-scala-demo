package demo

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{
    GlobalKTable,
    JoinWindows,
    TimeWindows,
    Windowed
}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object Main {

    object Domain {
        type UserId = String
        type Profile = String
        type Product = String
        type OrderId = String
        type Status = String

        case class Order(
            orderId: OrderId,
            userId: UserId,
            products: List[Product],
            amount: Double
        )
        case class Discount(profile: Profile, amount: Double)
        case class Payment(orderId: OrderId, status: Status)
    }

    object Topics {
        final val OrdersByUser = "orders-by-user"
        final val DiscountProfilesByUser = "discount-profiles-by-user"
        final val Discounts = "discounts"
        final val Orders = "orders"
        final val Payments = "payments"
        final val PaidOrders = "paid-orders"
    }

    import Domain._
    import Topics._

    implicit def serde[A >: Null: Decoder: Encoder]: Serde[A] = {
        val serializer = (a: A) => a.asJson.noSpaces.getBytes()
        val deserializer = (bytes: Array[Byte]) => {
            val string = new String(bytes)
            decode[A](string).toOption
        }
        Serdes.fromFn[A](serializer, deserializer)
    }

    def main(args: Array[String]): Unit = {
        // topology
        val builder = new StreamsBuilder()

        // KStream
        val usersOrdersStream: KStream[UserId, Order] =
            builder.stream[UserId, Order](OrdersByUser)

        // KTable : maintained into the broker | cleanup policy must be compact
        val userProfilesTable: KTable[UserId, Profile] =
            builder.table[UserId, Profile](DiscountProfilesByUser)

        // GlobalKTable - copied to all the nodes - joining this with a KTable is cheaper
        val discountProfilesGTable: GlobalKTable[Profile, Discount] =
            builder.globalTable[Profile, Discount](Discounts)

        // Example transformations: filter, map, flatmap
        val expensiveOrders = usersOrdersStream.filter { (userId, order) =>
            order.amount > 1000
        }
        val listOfProducts = usersOrdersStream.mapValues { order =>
            order.products
        }
        val productsStream = usersOrdersStream.flatMapValues(_.products)

        // Some shuffling: join
        val ordersWithUserProfiles = usersOrdersStream.join(userProfilesTable) {
            (order, profile) => (order, profile)
        }
        val discountedOrdersStream =
            ordersWithUserProfiles.join(discountProfilesGTable)(
              { case (userId, (order, profile)) =>
                  profile
              }, // key picked from left stream
              { case ((order, profile), discount) =>
                  order.copy(amount =
                      order.amount - discount.amount
                  ) // values of the matched records
              }
            )

        val ordersStream =
            discountedOrdersStream.selectKey((userId, order) => order.orderId)
        val paymentsStream = builder.stream[OrderId, Payment](Payments)
        // when joining 2 streams instead of stream with static table use a window

        val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
        val joinOrdersPayments = (order: Order, payment: Payment) =>
            if (payment.status == "PAID") Option(order) else Option.empty[Order]
        val ordersPaid = ordersStream
            .join(paymentsStream)(joinOrdersPayments, joinWindow)
            .flatMapValues(maybeOrder => maybeOrder.toIterable)

        // sink
        ordersPaid.to(PaidOrders)

        val topology = builder.build()

        val props = new Properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass())

        println(topology.describe())

        val application = new KafkaStreams(topology, props)
        application.start()
    }
}

