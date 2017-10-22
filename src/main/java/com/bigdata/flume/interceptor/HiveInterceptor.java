package com.bigdata.flume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by angelrojoperez on 20/10/17
 */
public class HiveInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(HiveInterceptor.class);

    private final String hiveJdbcUrl;
    private final String statement;
    private List<String> partitionCache;

    public HiveInterceptor(String hiveJdbcUrl, String statement) {
        this.hiveJdbcUrl = hiveJdbcUrl;
        this.statement = statement;
        this.partitionCache = new LinkedList<>();
    }

    @Override
    public void initialize() {
        try {
            String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(HIVE_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load the hive jdbc driver");
        }
    }

   @Override
   public Event intercept(Event event) {
        if (event != null) {
            String eventYear = event.getHeaders().get("event_year");
            String eventMonth = event.getHeaders().get("event_month");
            String eventDay = event.getHeaders().get("event_day");
            String partitionKey = eventYear + "," + eventMonth + "," + eventDay;

            if (!(this.partitionCache.contains(partitionKey))) {
                logger.info("add new hive partitionKey : " + partitionKey);
                partitionCache.add(partitionKey);
                try {
                    executeQuery();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return event;
   }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = new LinkedList<>();
        for(Event e : events) {
            intercepted.add(intercept(e));
        }
        return intercepted;
    }

    @Override
    public void close() {}


    private void executeQuery() throws SQLException {
        Connection connection = DriverManager.getConnection(this.hiveJdbcUrl);
        Statement statement = connection.createStatement();
        statement.execute(this.statement);
        logger.info("HIVE execute statement >>> " + statement);
        connection.close();
    }
}
