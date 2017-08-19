package com.hortonworks.examples.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Run a query forever in N number of threads.
 */
public class RunConcurrent {
    private static final Object monitor = new Object();
    private Connection connection = null;
    private String sql = null;

    private RunConcurrent() {
        Properties props = new Properties();
        try {
            props.load(getClass().getClassLoader().getResourceAsStream("query.properties"));
            String url = props.getProperty("db.url");
            String username = props.getProperty("db.username");
            String password = props.getProperty("db.password");
            this.sql = props.getProperty("sql");

            Class.forName(props.getProperty("db.driver"));
            this.connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connection Established...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        final RunConcurrent app = new RunConcurrent();
        int threadCount = 3;
        if (args.length > 0) {
            threadCount = Integer.parseInt(args[0]);
        }
        ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
        Runnable doQuery = new Runnable() {
            public void run() {
                while (true) {
                    try {
                        System.out.println(Thread.currentThread().getName() + " starting a query...");
                        ResultSet rs = null;
                        Statement stmt = null;
                        synchronized (monitor) {
                            stmt = app.connection.createStatement();
                        }
                        rs = stmt.executeQuery(app.sql);
                        while (rs.next()) {
                            rs.getObject(1); // to ensure retrieval
                        }
                        System.out.println(Thread.currentThread().getName() + " completed a query...");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
        };

        for (int i = 0; i < threadCount; i++) {
            tpe.execute(doQuery);
        }
    }
}
