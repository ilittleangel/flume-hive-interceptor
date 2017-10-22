package com.bigdata.flume.interceptor;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

/**
 * Created by angelrojoperez on 22/10/17
 */
public class HiveInterceptorBuilder implements Interceptor.Builder {

    private String hiveJdbcUrl;
    private String statement;

    @Override
    public HiveInterceptor build() {
        return new HiveInterceptor(this.hiveJdbcUrl, this.statement);
    }

    @Override
    public void configure(Context context) {
        hiveJdbcUrl = Preconditions.checkNotNull(context.getString("hive.jdbcUrl"),"Hive JDBC URL is required");
        statement = Preconditions.checkNotNull(context.getString("hive.statement"),"Hive STATEMENT is required");

    }
}
