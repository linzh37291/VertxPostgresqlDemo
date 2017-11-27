package com.lin.demo.database;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import com.lin.demo.BaseCRUD;
import com.lin.demo.http.ArticleRouter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;  
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

public class ArticleDaoVerticle extends AbstractVerticle implements BaseCRUD {
	public static final Logger LOGGER = LoggerFactory.getLogger(ArticleRouter.class);
	private SQLClient postgreSQLClient;
	private HashMap<SqlQuery, String> sqlQueries;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		LOGGER.info("部署 ArticleDaoVerticle");
		sqlQueries = loadSqlQueries();


		JsonObject postgreSQLClientConfig = new JsonObject()
				.put("host","localhost")
				.put("port",5432)
				.put("username", "postgres")
				.put("password",  "qq451791119")
				.put("database", "vertx_postgresql")
				.put("max_pool_size", 30)  ;  
		postgreSQLClient = PostgreSQLClient.createShared(vertx, postgreSQLClientConfig);

		postgreSQLClient.getConnection(ar -> {
			if (ar.failed()) {
				LOGGER.error("Could not open a database connection", ar.cause());

			} else {
				SQLConnection connection = ar.result();
				connection.execute(sqlQueries.get(SqlQuery.CREATE_TABLE), create -> {
					connection.close();
					if (create.failed()) {
						LOGGER.error("Database preparation error", create.cause());

					} else {
						LOGGER.info("注册Dao方法到事件总线");
						vertx.eventBus().consumer("dao://article/find", this::find);
						vertx.eventBus().consumer("dao://article/add", this::add);
						vertx.eventBus().consumer("dao://article/update", this::update);
						vertx.eventBus().consumer("dao://article/delete", this::delete);
						vertx.eventBus().consumer("dao://article/findAll", this::findAll);
					}
				});
			}
		});

	}

	public void find(Message<JsonObject> msg) {
		String id = msg.body().getString("fld_id");
		JsonArray params = new JsonArray().add(id);
		LOGGER.info(params.toBuffer());
		postgreSQLClient.getConnection(con -> {

			if (con.succeeded()) {
				SQLConnection sqlConnection = con.result();

				sqlConnection.queryWithParams(sqlQueries.get(SqlQuery.GET), params, res -> {

					if (res.succeeded()) {

						msg.reply(res.result().toJson());
					} else {
						LOGGER.info(res.cause());
					}
				});
			}
		});
	}

	public void add(Message<JsonObject> msg) {
		LOGGER.info("执行add方法");
		JsonArray params = new JsonArray().add(msg.body().getString("fld_id")).add(msg.body().toString());
		LOGGER.info(params.toString());
		postgreSQLClient.getConnection(con -> {

			if (con.succeeded()) {
				SQLConnection sqlConnection = con.result();

				sqlConnection.updateWithParams(sqlQueries.get(SqlQuery.ADD), params, res -> {

					if (res.succeeded()) {

						msg.reply(res.result().toJson());
					} else {
						LOGGER.info(res.cause());
					}

				});
		
				
			}

		});
	}

	public void update(Message<JsonObject> msg) {

		String id = msg.body().getString("fld_id", "");
		
		JsonArray params = new JsonArray().add(msg.body().toString()).add(id);
		postgreSQLClient.getConnection(con -> {

			if (con.succeeded()) {
				SQLConnection sqlConnection = con.result();

				sqlConnection.updateWithParams(sqlQueries.get(SqlQuery.UPDATE), params, res -> {
					if (res.succeeded()) {

						msg.reply(res.result().toJson());
					} else {
						LOGGER.info(res.cause());
					}
				});
			}

		});
	}

	public void delete(Message<JsonObject> msg) {
		String id = msg.body().getString("fld_id", "");

		JsonArray params = new JsonArray().add(id);
		postgreSQLClient.getConnection(con -> {

			if (con.succeeded()) {
				SQLConnection sqlConnection = con.result();

				sqlConnection.updateWithParams(sqlQueries.get(SqlQuery.DELETE), params, res -> {

					if (res.succeeded()) {

						msg.reply(res.result().toJson());
					} else {
						LOGGER.info(res.cause());
					}
				});
			}

		});
	}

	public void findAll(Message<JsonObject> msg) {

		postgreSQLClient.getConnection(con -> {

			if (con.succeeded()) {
				SQLConnection sqlConnection = con.result();

				sqlConnection.query(sqlQueries.get(SqlQuery.ALL), res -> {
					if (res.succeeded()) {

						msg.reply(res.result().toJson());
					} else {
						LOGGER.info(res.cause());
					}
				});
			}

		});
	}

	/*
	 * Note: this uses blocking APIs, but data is small...
	 */
	public HashMap<SqlQuery, String> loadSqlQueries() throws IOException {

		HashMap<SqlQuery, String> sqlQueries = new HashMap<>();
		sqlQueries.put(SqlQuery.CREATE_TABLE,
				"create table if not exists tbl_article " + 
				"(" + 
				"  fld_id uuid primary key , " + 
				"  fld_json json " + 
				"); ");
		sqlQueries.put(SqlQuery.ALL, "select * from tbl_article");
		sqlQueries.put(SqlQuery.GET, "select fld_id,fld_json from tbl_article where fld_id = ?");
		sqlQueries.put(SqlQuery.ADD, "insert into tbl_article values (?, ?)");
		sqlQueries.put(SqlQuery.UPDATE, "update tbl_article set fld_json = ? where fld_id = ?");
		sqlQueries.put(SqlQuery.DELETE, "delete from tbl_article where fld_id = ?");
		return sqlQueries;
	}

}