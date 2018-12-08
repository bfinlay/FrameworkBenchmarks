import com.github.jasync.sql.db.Configuration
import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.general.ArrayRowData
import com.github.jasync.sql.db.mysql.pool.MySQLConnectionFactory
import com.github.jasync.sql.db.pool.PartitionedConnectionPool
import com.github.jasync.sql.db.pool.PoolConfiguration
import io.reactivex.Flowable
import io.reactivex.rxkotlin.subscribeBy
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import vertx.model.Message
import vertx.model.World
import java.io.File
import java.nio.file.Files
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

class AppMySqlAlt : AbstractVerticle(), Handler<HttpServerRequest> {

    private var dateString: CharSequence? = null

    private var server: HttpServer? = null

    private var connection: Connection? = null

    @Throws(Exception::class)
    override fun start() {
        val port = 8080
        server = vertx.createHttpServer(HttpServerOptions())
        server!!.requestHandler(this@AppMySqlAlt).listen(port)
        dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()))
        val config = config()
        vertx.setPeriodic(1000) { handler -> dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now())) }

        val poolConfiguration = PoolConfiguration(
                100, // maxObjects
                TimeUnit.MINUTES.toMillis(15), // maxIdle
                10000, // maxQueueSize
                TimeUnit.SECONDS.toMillis(30) // validationInterval
        )

        connection = PartitionedConnectionPool(
                MySQLConnectionFactory(Configuration(
                        username = config.getString("username"),
                        password = config.getString("password"),
                        host = config.getString("host"),
                        port = 3306,
                        database = config.getString("database")
                )),
                poolConfiguration,
                10
        )

        connection!!.connect().get()
    }

    override fun handle(request: HttpServerRequest) {
        when (request.path()) {
            vertx.AppMySqlAlt.Companion.PATH_PLAINTEXT -> handlePlainText(request)
            vertx.AppMySqlAlt.Companion.PATH_JSON -> handleJson(request)
            vertx.AppMySqlAlt.Companion.PATH_DB -> handleDb(request)
            vertx.AppMySqlAlt.Companion.PATH_QUERIES -> handleQueries(request)
//            PATH_UPDATES -> handleUpdates(request)
//            PATH_FORTUNES -> handleFortunes(request)
            else -> {
                request.response().statusCode = 404
                request.response().end()
            }
        }
    }

    override fun stop() {
        server!!.close()
    }

    private fun handlePlainText(request: HttpServerRequest) {
        val response = request.response()
        val headers = response.headers()
        headers
                .add(vertx.AppMySqlAlt.Companion.HEADER_CONTENT_TYPE, vertx.AppMySqlAlt.Companion.RESPONSE_TYPE_PLAIN)
                .add(vertx.AppMySqlAlt.Companion.HEADER_SERVER, vertx.AppMySqlAlt.Companion.SERVER)
                .add(vertx.AppMySqlAlt.Companion.HEADER_DATE, dateString)
                .add(vertx.AppMySqlAlt.Companion.HEADER_CONTENT_LENGTH, vertx.AppMySqlAlt.Companion.HELLO_WORLD_LENGTH)
        response.end(vertx.AppMySqlAlt.Companion.HELLO_WORLD_BUFFER)
    }

    private fun handleJson(request: HttpServerRequest) {
        val response = request.response()
        val headers = response.headers()
        headers
                .add(vertx.AppMySqlAlt.Companion.HEADER_CONTENT_TYPE, vertx.AppMySqlAlt.Companion.RESPONSE_TYPE_JSON)
                .add(vertx.AppMySqlAlt.Companion.HEADER_SERVER, vertx.AppMySqlAlt.Companion.SERVER)
                .add(vertx.AppMySqlAlt.Companion.HEADER_DATE, dateString)
        response.end(Message("Hello, World!").toBuffer())
    }

    private fun handleDb(req: HttpServerRequest) {
        Flowable.fromFuture(connection!!.connect())
                .flatMap {
                    Flowable.fromFuture(it.sendPreparedStatement(vertx.AppMySqlAlt.Companion.SELECT_WORLD, listOf(vertx.AppMySqlAlt.Companion.randomWorld())))
                }
                .subscribeBy {
                            val r = it.rows!![0] as ArrayRowData
                            val world = World(r["id"] as Int, r["randomnumber"] as Int)
                            req.response()
                                    .putHeader(HttpHeaders.SERVER, vertx.AppMySqlAlt.Companion.SERVER)
                                    .putHeader(HttpHeaders.DATE, dateString)
                                    .putHeader(HttpHeaders.CONTENT_TYPE, vertx.AppMySqlAlt.Companion.RESPONSE_TYPE_JSON)
                                    .end(Json.encode(world))
                        }
    }

    private fun handleQueries(req: HttpServerRequest) {
        val worlds = JsonArray()

        val queries = vertx.AppMySqlAlt.Companion.getQueries(req)

//        for (query in 0..queries) {
//            val future = connection!!.sendPreparedStatement(SELECT_WORLD, listOf(randomWorld()))
//            val r = future.get().rows!![0] as ArrayRowData
//            val world = World(r["id"] as Int, r["randomnumber"] as Int)
//
//            worlds.add(JsonObject().put("id", "" + r["id"]).put("randomNumber", "" + r["randomnumber"]))
//            r.get("id");
//
//        }

        Flowable.range(0, queries)
                .flatMap {
                    Flowable.fromFuture(connection!!.connect())
                            .flatMap {
                                Flowable.fromFuture(it.sendPreparedStatement(vertx.AppMySqlAlt.Companion.SELECT_WORLD, listOf(vertx.AppMySqlAlt.Companion.randomWorld())))
                            }
                            .map {
                                it.rows!![0] as ArrayRowData
                            }
                }
                .subscribeBy(
                        onNext = {
                            // val r = it.rows!![0] as ArrayRowData
                            worlds.add(JsonObject().put("id", "" + it["id"]).put("randomNumber", "" + it["randomnumber"]))
                        },
                        onComplete = {
                            req.response().putHeader(HttpHeaders.SERVER, vertx.AppMySqlAlt.Companion.SERVER)
                                    .putHeader(HttpHeaders.DATE, dateString)
                                    .putHeader(HttpHeaders.CONTENT_TYPE, vertx.AppMySqlAlt.Companion.RESPONSE_TYPE_JSON)
                                    .end(worlds.encode())
                        }

                )

//        Flowable.range(0, queries)
//                .flatMap({ i ->
//                    client!!.select(SELECT_WORLD)
//                            .parameter(randomWorld())
//                            .getAs(Int::class.java, Int::class.java)
//                })
//                .subscribe(
//                        { x -> worlds.add(JsonObject().put("id", "" + x._1()).put("randomNumber", "" + x._2())) },
//                        { /* do nothing */ throwable -> },
//                        {
//                            req.response().putHeader(HttpHeaders.SERVER, SERVER)
//                                    .putHeader(HttpHeaders.DATE, dateString)
//                                    .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
//                                    .end(worlds.encode())
//                        }
//                )
    }
//
//
//    private fun handleUpdates(req: HttpServerRequest) {
//        val queries = getQueries(req)
//        val worlds = JsonArray()
//
//        Flowable.range(0, queries)
//                .flatMap({ i ->
//                    val id = randomWorld()
//
//                    client!!.select(SELECT_WORLD)
//                            .parameter(id)
//                            .getAs(Int::class.java, Int::class.java)
//                            .map({ x ->
//                                val world = World(x._1(), randomWorld())
//                                worlds.add(JsonObject().put("id", world.id).put("randomNumber", world.randomNumber))
//                                world
//                            })
//                })
//                .flatMap({ world ->
//                    client!!.update(UPDATE_WORLD)
//                            .batchSize(queries)
//                            .parameters(world.getRandomNumber(), world.getId())
//                            .counts()
//                })
//                .subscribe(
//                        { /* do nothing onNext */ nextItem -> },
//                        { err ->
//                            logger.error("", err)
//                            req.response().setStatusCode(500).end(err.getMessage())
//                        },
//                        {
//                            req.response()
//                                    .putHeader(HttpHeaders.SERVER, SERVER)
//                                    .putHeader(HttpHeaders.DATE, dateString)
//                                    .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
//                                    .end(worlds.toBuffer())
//                        })
//    }
//
//
//    private fun handleFortunes(req: HttpServerRequest) {
//        val response = req.response()
//        val fortunes = ArrayList<Fortune>()
//
//        client!!.select(SELECT_FORTUNE)
//                .getAs(Int::class.java, String::class.java)
//                .subscribe(
//                        { x -> fortunes.add(Fortune(x._1(), x._2())) },
//                        { err ->
//                            logger.error("", err)
//                            req.response().setStatusCode(500).end(err.getMessage())
//                        },
//                        {
//                            fortunes.add(Fortune(0, "Additional fortune added at request time."))
//                            Collections.sort(fortunes)
//                            response
//                                    .putHeader(HttpHeaders.SERVER, SERVER)
//                                    .putHeader(HttpHeaders.DATE, dateString)
//                                    .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_HTML)
//                                    .end(FortunesTemplate.template(fortunes).render().toString())
//                        }
//                )
//    }

    companion object {

        /**
         * Returns the value of the "queries" getRequest parameter, which is an integer
         * bound between 1 and 500 with a default value of 1.
         *
         * @param request the current HTTP request
         * @return the value of the "queries" parameter
         */
        internal fun getQueries(request: HttpServerRequest): Int {
            val param = request.getParam("queries") ?: return 1

            return try {
                val parsedValue = Integer.parseInt(param)
                Math.min(500, Math.max(1, parsedValue))
            } catch (e: NumberFormatException) {
                1
            }

        }

        private var logger = LoggerFactory.getLogger(AppMySqlAlt::class.java.name)

        private const val PATH_PLAINTEXT = "/plaintext"
        private const val PATH_JSON = "/json"
        private const val PATH_DB = "/db"
        private const val PATH_QUERIES = "/queries"
        private const val PATH_UPDATES = "/updates"
        private const val PATH_FORTUNES = "/fortunes"

        private val RESPONSE_TYPE_PLAIN = HttpHeaders.createOptimized("text/plain")
        private val RESPONSE_TYPE_HTML = HttpHeaders.createOptimized("text/html; charset=UTF-8")
        private val RESPONSE_TYPE_JSON = HttpHeaders.createOptimized("application/json")

        private const val HELLO_WORLD = "Hello, world!"
        private val HELLO_WORLD_BUFFER = Buffer.buffer(HELLO_WORLD)

        private val HEADER_SERVER = HttpHeaders.createOptimized("server")
        private val HEADER_DATE = HttpHeaders.createOptimized("date")
        private val HEADER_CONTENT_TYPE = HttpHeaders.createOptimized("content-type")
        private val HEADER_CONTENT_LENGTH = HttpHeaders.createOptimized("content-length")

        private val HELLO_WORLD_LENGTH = HttpHeaders.createOptimized("" + HELLO_WORLD.length)
        private val SERVER = HttpHeaders.createOptimized("vert.x")

        private const val UPDATE_WORLD = "UPDATE world SET randomnumber=? WHERE id=?"
        private const val SELECT_WORLD = "SELECT id, randomnumber from WORLD where id=?"
        private const val SELECT_FORTUNE = "SELECT id, message from FORTUNE"

        /**
         * Returns a random integer that is a suitable value for both the `id`
         * and `randomNumber` properties of a world object.
         *
         * @return a random world number
         */
        private fun randomWorld(): Int {
            return 1 + ThreadLocalRandom.current().nextInt(10000)
        }

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val config = JsonObject(String(Files.readAllBytes(File(args[0]).toPath())))
            val procs = Runtime.getRuntime().availableProcessors()
            val vertx = Vertx.vertx()
            vertx.exceptionHandler { err -> err.printStackTrace() }
            vertx.deployVerticle(AppMySqlAlt::class.java.name,
                    DeploymentOptions().setInstances(procs * 2).setConfig(config)) { event ->
                if (event.succeeded()) {
                    vertx.AppMySqlAlt.Companion.logger.debug("Your Vert.x application is started!")
                } else {
                    vertx.AppMySqlAlt.Companion.logger.error("Unable to start your application", event.cause())
                }
            }
        }
    }
}
