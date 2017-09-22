package com.hortonworks.examples.hive;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * create tables and drop in multi threads
 */
public class DropTest {
    private Connection connection = null;
    private String sql = null;
    private int sleepTime = 3000;
    private static String dbName;
    private int numTables = 100;

    private DropTest() {
        Properties props = new Properties();
        try {
            props.load(getClass().getClassLoader().getResourceAsStream("droptest.properties"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        String url = props.getProperty("db.url");
        String username = props.getProperty("db.username");
        String password = props.getProperty("db.password");
        this.dbName = this.dbName == null ? props.getProperty("db.prefix") + ((int) Math.ceil(Math.random() * 10000)) : dbName;
        this.sql = props.getProperty("sql");
        this.sleepTime = Integer.parseInt(props.getProperty("sleep.ms"));
        this.numTables = Integer.parseInt(props.getProperty("num.tables"));
        try {
            Class.forName(props.getProperty("db.driver"));
            this.connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connection Established...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    protected void createTables() throws SQLException {
        connection.createStatement().executeUpdate("create database " + this.dbName);
        for (int i = 0; i < numTables; i++) {
            String curTbl = this.dbName +  ".tbl" + i;
            System.out.println("Creating table " + curTbl);
            connection.createStatement().executeUpdate("create table " + curTbl + " as " + this.sql);
            System.out.println("Created table " + curTbl);
            connection.createStatement().executeUpdate("grant select on table " + curTbl + " to user zookeeper");
            System.out.println("Granted permissions to table " + curTbl);
        }
    }

    public static void main(String[] args) throws Exception {
        DropTest app = new DropTest();
        int threadCount = 3;
        if (args.length > 0) {
            threadCount = Integer.parseInt(args[0]);
        }
        app.createTables();
        final CyclicBarrier cb = new CyclicBarrier(threadCount);
        Runnable doQuery = new Runnable() {
            public void run() {
                DropTest tester = new DropTest();
                try {
                    cb.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }

                for (int i = 0; i < tester.numTables; i++) {
                    String tableName = tester.dbName +  ".tbl" + i;
                    try {
                        System.out.println(Thread.currentThread().getName() + " starting a query to drop view " + tableName);
                        try {
                            tester.connection.createStatement().executeUpdate("drop view if exists " + tableName);
                        } catch (Exception e) {
                            System.err.println("Problem dropping view (this is expected): " + e.getMessage());
                        }
                        System.out.println(Thread.currentThread().getName() + " completed drop view attempt...");
                        System.out.println(Thread.currentThread().getName() + " starting a query to drop table " + tableName);
                        try {
                            tester.connection.createStatement().executeUpdate("drop table if exists " + tableName);
                        } catch (Exception e) {
                            System.err.println("Problem dropping table (this is expected): " + e.getMessage());
                        }
                        System.out.println(Thread.currentThread().getName() + " completed drop table attempt...");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
        };

        for (int i = 0; i < threadCount; i++) {
            new Thread(doQuery).start();
        }
    }
}
