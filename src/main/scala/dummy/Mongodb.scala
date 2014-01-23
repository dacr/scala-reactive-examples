package dummy

object Mongodb {

  def connect() {
    import reactivemongo.api._
    import scala.concurrent.ExecutionContext.Implicits.global

    // gets an instance of the driver
    // (creates an actor system)
    val driver = new MongoDriver
    val connection = driver.connection(List("localhost"))

    // Gets a reference to the database "plugin"
    val db = connection("plugin")

    // Gets a reference to the collection "acoll"
    // By default, you get a BSONCollection.
    val collection = db("acoll")
  }

}

