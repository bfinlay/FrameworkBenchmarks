package vertx;

import io.reactivex.Flowable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import vertx.model.Fortune;
import vertx.model.Message;
import vertx.model.World;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class AppMySql extends AbstractVerticle implements Handler<HttpServerRequest> {

  /**
   * Returns the value of the "queries" getRequest parameter, which is an integer
   * bound between 1 and 500 with a default value of 1.
   *
   * @param request the current HTTP request
   * @return the value of the "queries" parameter
   */
  static int getQueries(HttpServerRequest request) {
    String param = request.getParam("queries");

    if (param == null) {
      return 1;
    }
    try {
      int parsedValue = Integer.parseInt(param);
      return Math.min(500, Math.max(1, parsedValue));
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  private class closeHandler implements Handler<Void> {
      private HttpServerRequest req;

      closeHandler(HttpServerRequest request) {
          this.req = request;
      }

      @Override
      public void handle(Void e) {
          logger.debug(String.format("HTTP response was closed by client: req=%s", this.req));
      }
  }

  static Logger logger = LoggerFactory.getLogger(AppMySql.class.getName());

  private static final String PATH_PLAINTEXT = "/plaintext";
  private static final String PATH_JSON = "/json";
  private static final String PATH_DB = "/db";
  private static final String PATH_QUERIES = "/queries";
  private static final String PATH_UPDATES = "/updates";
  private static final String PATH_FORTUNES = "/fortunes";

  private static final CharSequence RESPONSE_TYPE_PLAIN = HttpHeaders.createOptimized("text/plain");
  private static final CharSequence RESPONSE_TYPE_HTML = HttpHeaders.createOptimized("text/html; charset=UTF-8");
  private static final CharSequence RESPONSE_TYPE_JSON = HttpHeaders.createOptimized("application/json");

  private static final String HELLO_WORLD = "Hello, world!";
  private static final Buffer HELLO_WORLD_BUFFER = Buffer.buffer(HELLO_WORLD);

  private static final CharSequence HEADER_SERVER = HttpHeaders.createOptimized("server");
  private static final CharSequence HEADER_DATE = HttpHeaders.createOptimized("date");
  private static final CharSequence HEADER_CONTENT_TYPE = HttpHeaders.createOptimized("content-type");
  private static final CharSequence HEADER_CONTENT_LENGTH = HttpHeaders.createOptimized("content-length");

  private static final CharSequence HELLO_WORLD_LENGTH = HttpHeaders.createOptimized("" + HELLO_WORLD.length());
  private static final CharSequence SERVER = HttpHeaders.createOptimized("vert.x");

  private static final String UPDATE_WORLD = "UPDATE world SET randomnumber=? WHERE id=?";
  private static final String BATCH_UPDATE_WORLD = "UPDATE world SET randomnumber = CASE %s END where id in (%s)";
  private static final String BATCH_UPDATE_WORLD_VALUE = "WHEN id=? THEN ?";
  private static final String BATCH_INSERT_ON_UPDATE_WORLD = "INSERT INTO world (id, randomnumber) VALUES %s ON DUPLICATE KEY UPDATE randomnumber = values(randomnumber)";
  private static final String BATCH_INSERT_ON_UPDATE_WORLD_VALUE = "(?,?)";
  private static final String SELECT_WORLD = "SELECT id, randomnumber from WORLD where id=?";
  private static final String SELECT_FORTUNE = "SELECT id, message from FORTUNE";

  private CharSequence dateString;

  private HttpServer server;

  private Database client;

  @Override
  public void start() throws Exception {
    int port = 8080;
    server = vertx.createHttpServer(new HttpServerOptions());
    server.requestHandler(AppMySql.this).listen(port);
    dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    JsonObject config = config();
    vertx.setPeriodic(1000, handler -> {
      dateString = HttpHeaders.createOptimized(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
    });

    String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s?user=%s&password=%s&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
            config.getString("host"),
            config.getString("database"),
            config.getString("username"),
            config.getString("password")
    );

    client = Database.from(jdbcUrl, 1);
  }

  @Override
  public void handle(HttpServerRequest request) {
    switch (request.path()) {
      case PATH_PLAINTEXT:
        handlePlainText(request);
        break;
      case PATH_JSON:
        handleJson(request);
        break;
      case PATH_DB:
        handleDb(request);
        break;
      case PATH_QUERIES:
        handleQueries(request);
        break;
      case PATH_UPDATES:
        handleUpdatesWithBatch(request);
        break;
      case PATH_FORTUNES:
        handleFortunes(request);
        break;
      default:
        request.response().setStatusCode(404);
        request.response().end();
        break;
    }
  }

  @Override
  public void stop() {
    if (server != null) server.close();
  }

  private void handlePlainText(HttpServerRequest request) {
    HttpServerResponse response = request.response();
    MultiMap headers = response.headers();
    headers
        .add(HEADER_CONTENT_TYPE, RESPONSE_TYPE_PLAIN)
        .add(HEADER_SERVER, SERVER)
        .add(HEADER_DATE, dateString)
        .add(HEADER_CONTENT_LENGTH, HELLO_WORLD_LENGTH);
    response.end(HELLO_WORLD_BUFFER);
  }

  private void handleJson(HttpServerRequest request) {
    HttpServerResponse response = request.response();
    MultiMap headers = response.headers();
    headers
        .add(HEADER_CONTENT_TYPE, RESPONSE_TYPE_JSON)
        .add(HEADER_SERVER, SERVER)
        .add(HEADER_DATE, dateString);
    response.end(new Message("Hello, World!").toBuffer());
  }

  /**
   * Returns a random integer that is a suitable value for both the {@code id}
   * and {@code randomNumber} properties of a world object.
   *
   * @return a random world number
   */
  private static int randomWorld() {
    return 1 + ThreadLocalRandom.current().nextInt(10000);
  }

  private void handleDb(HttpServerRequest req) {
    client.select(SELECT_WORLD)
            .parameter(randomWorld())
            .getAs(Integer.class, Integer.class)
            .subscribe(x -> {
              World world = new World(x._1(), x._2());
              req.response()
                      .putHeader(HttpHeaders.SERVER, SERVER)
                      .putHeader(HttpHeaders.DATE, dateString)
                      .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
                      .end(Json.encode(world));
                    }
            );
  }

  private void handleQueries(HttpServerRequest req) {
    JsonArray worlds = new JsonArray();

    final int queries = getQueries(req);
    Flowable.range(0, queries)
            .flatMap(i -> {
              return client.select(SELECT_WORLD)
                      .parameter(randomWorld())
                      .getAs(Integer.class, Integer.class);
            })
            .subscribe(
                    x -> {
                      worlds.add(new JsonObject().put("id", "" + x._1()).put("randomNumber", "" + x._2()));
                    },
                    throwable -> { /* do nothing */ },
                    () -> {
                            req.response().putHeader(HttpHeaders.SERVER, SERVER)
                            .putHeader(HttpHeaders.DATE, dateString)
                            .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
                            .end(worlds.encode());
                    }
            );
  }


  private void handleUpdates(HttpServerRequest req) {
    final int queries = getQueries(req);
    JsonArray worlds = new JsonArray();
    req.response().closeHandler(new closeHandler(req));

    logger.debug(String.format("update request started: req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));

    Flowable.range(0, queries)
            .flatMap(i -> {
              int id = randomWorld();

              return client.select(SELECT_WORLD)
                      .parameter(id)
                      .getAs(Integer.class, Integer.class)
                      .map(x -> {
                        World world = new World(x._1(), randomWorld());
                        worlds.add(new JsonObject().put("id", world.getId()).put("randomNumber", world.getRandomNumber()));
                        return new Tuple2<Integer, World>(i, world);
                      });
            })
            .flatMap(t -> client.update(UPDATE_WORLD)
                    .batchSize(queries)
                    .parameters(t._2().getRandomNumber(), t._2().getId())
                    .counts()
                    .map(x -> {
                        return new Tuple3<Integer, World, Integer>(t._1(), t._2(), x);
                    })
            )
            .subscribe(
                    nextItem -> { /* do nothing onNext */
                      logger.debug(String.format("update complete: req=%s, Q=%d, N=%d, World(%d, %d), updates:%d, thread:%s, port:%d", req, queries, nextItem._1(), nextItem._2().getId(), nextItem._2().getRandomNumber(), nextItem._3(), Thread.currentThread(), req.remoteAddress().port()));
                    },
                    err -> {
                      logger.error(String.format("handleUpdates subscribe error: req=%s, error=%s", req, err));
                      req.response().setStatusCode(500).end(err.getMessage());
                    },
                    () -> {
                      if (req.response().closed()) {
                          logger.debug(String.format("update complete - connection closed by client: req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                      }
                      else {
                          req.response()
                                  .putHeader(HttpHeaders.SERVER, SERVER)
                                  .putHeader(HttpHeaders.DATE, dateString)
                                  .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
                                  .end(worlds.toBuffer());
                          logger.debug(String.format("all updates successfully completed, req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                      }
                    });
  }

    private void handleUpdatesWithBatchInsertOnUpdate(HttpServerRequest req) {
        final int queries = getQueries(req);
        JsonArray worlds = new JsonArray();
        ArrayList<Integer> batchWorlds = new ArrayList<>();

        req.response().closeHandler(new closeHandler(req));

        logger.debug(String.format("update request started: req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));

        Flowable.range(0, queries)
                .flatMap(i -> {
                    int id = randomWorld();

                    return client.select(SELECT_WORLD)
                            .parameter(id)
                            .getAs(Integer.class, Integer.class)
                            .map(x -> {
                                World world = new World(x._1(), randomWorld());
                                worlds.add(new JsonObject().put("id", world.getId()).put("randomNumber", world.getRandomNumber()));
                                batchWorlds.add(world.getId());
                                batchWorlds.add(world.getRandomNumber());
                                return world;
                            });
                })
                .takeLast(1)
                .flatMap(w -> {
                    logger.debug(String.format("update all select complete, req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                    String batchUpdate = String.format(BATCH_INSERT_ON_UPDATE_WORLD, String.join(",", Collections.nCopies(queries, BATCH_INSERT_ON_UPDATE_WORLD_VALUE)));
                    return client.update(batchUpdate)
                            .parameters(batchWorlds)
                            .counts();
                    }
                )
                .subscribe(
                        nextItem -> { /* do nothing onNext */
//                            logger.debug(String.format("update complete: req=%s, Q=%d, N=%d, World(%d, %d), updates:%d, thread:%s, port:%d", req, queries, nextItem._1(), nextItem._2().getId(), nextItem._2().getRandomNumber(), nextItem._3(), Thread.currentThread(), req.remoteAddress().port()));
                            logger.debug(String.format("update complete: req=%s, Q=%d, rowsAffected=%d, thread:%s, port:%d", req, queries, nextItem, Thread.currentThread(), req.remoteAddress().port()));

                        },
                        err -> {
                            logger.error(String.format("handleUpdates subscribe error: req=%s, error=%s", req, err));
                            req.response().setStatusCode(500).end(err.getMessage());
                        },
                        () -> {
                            if (req.response().closed()) {
                                logger.debug(String.format("update complete - connection closed by client: req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                            }
                            else {
                                req.response()
                                        .putHeader(HttpHeaders.SERVER, SERVER)
                                        .putHeader(HttpHeaders.DATE, dateString)
                                        .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
                                        .end(worlds.toBuffer());
                                logger.debug(String.format("all updates successfully completed, req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                            }
                        }
                );
    }

    private void handleUpdatesWithBatch(HttpServerRequest req) {
        final int queries = getQueries(req);
        JsonArray worlds = new JsonArray();
        ArrayList<Integer> batchWorlds = new ArrayList<>();
        ArrayList<Integer> batchWorldIds = new ArrayList<>();

        req.response().closeHandler(new closeHandler(req));

        logger.debug(String.format("update request started: req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));

        Flowable.range(0, queries)
                .flatMap(i -> {
                    int id = randomWorld();

                    return client.select(SELECT_WORLD)
                            .parameter(id)
                            .getAs(Integer.class, Integer.class)
                            .map(x -> {
                                World world = new World(x._1(), randomWorld());
                                worlds.add(new JsonObject().put("id", world.getId()).put("randomNumber", world.getRandomNumber()));
                                batchWorlds.add(world.getId());
                                batchWorlds.add(world.getRandomNumber());
                                batchWorldIds.add(world.getId());
                                return world;
                            });
                })
                .takeLast(1)
                .flatMap(w -> {
                            logger.debug(String.format("update all select complete, req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                            String batchUpdate = String.format(BATCH_UPDATE_WORLD,
                                    String.join(" ", Collections.nCopies(queries, BATCH_UPDATE_WORLD_VALUE)),
                                    String.join(",", Collections.nCopies(queries, "?"))
                            );
                            batchWorlds.addAll(batchWorldIds);
                            return client.update(batchUpdate)
                                    .parameters(batchWorlds)
                                    .counts();
                        }
                )
                .subscribe(
                        nextItem -> { /* do nothing onNext */
//                            logger.debug(String.format("update complete: req=%s, Q=%d, N=%d, World(%d, %d), updates:%d, thread:%s, port:%d", req, queries, nextItem._1(), nextItem._2().getId(), nextItem._2().getRandomNumber(), nextItem._3(), Thread.currentThread(), req.remoteAddress().port()));
                            logger.debug(String.format("update complete: req=%s, Q=%d, rowsAffected=%d, thread:%s, port:%d", req, queries, nextItem, Thread.currentThread(), req.remoteAddress().port()));

                        },
                        err -> {
                            logger.error(String.format("handleUpdates subscribe error: req=%s, error=%s", req, err));
                            req.response().setStatusCode(500).end(err.getMessage());
                        },
                        () -> {
                            if (req.response().closed()) {
                                logger.debug(String.format("update complete - connection closed by client: req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                            }
                            else {
                                req.response()
                                        .putHeader(HttpHeaders.SERVER, SERVER)
                                        .putHeader(HttpHeaders.DATE, dateString)
                                        .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_JSON)
                                        .end(worlds.toBuffer());
                                logger.debug(String.format("all updates successfully completed, req=%s, Q=%d, thread:%s, port:%d", req, queries, Thread.currentThread(), req.remoteAddress().port()));
                            }
                        }
                );
    }


  private void handleFortunes(HttpServerRequest req) {
    HttpServerResponse response = req.response();
    List<Fortune> fortunes = new ArrayList<>();

    client.select(SELECT_FORTUNE)
            .getAs(Integer.class, String.class)
            .subscribe(
                    x -> {
                      fortunes.add(new Fortune(x._1(), x._2()));
                    },
                    err -> {
                      logger.error("", err);
                      req.response().setStatusCode(500).end(err.getMessage());
                    },
                    () -> {
                      fortunes.add(new Fortune(0, "Additional fortune added at request time."));
                      Collections.sort(fortunes);
                      response
                        .putHeader(HttpHeaders.SERVER, SERVER)
                        .putHeader(HttpHeaders.DATE, dateString)
                        .putHeader(HttpHeaders.CONTENT_TYPE, RESPONSE_TYPE_HTML)
                        .end(FortunesTemplate.template(fortunes).render().toString());
                    }
            );
  }

  public static void main(String[] args) throws Exception {
    JsonObject config = new JsonObject(new String(Files.readAllBytes(new File(args[0]).toPath())));
    int procs = Runtime.getRuntime().availableProcessors();
    Vertx vertx = Vertx.vertx();
    vertx.exceptionHandler(err -> {
      err.printStackTrace();
    });
    vertx.deployVerticle(AppMySql.class.getName(),
        new DeploymentOptions().setInstances(procs * 2).setConfig(config), event -> {
        logger.error("error level");
        logger.info("info level");
        logger.debug("debug level");
          if (event.succeeded()) {
            logger.debug("Your Vert.x application is started!");
          } else {
            logger.error("Unable to start your application", event.cause());
          }
        });
  }
}
