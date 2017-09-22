Instructions - DropTest
===========

Create and populate the tables & views using instructions here: https://hortonworks.com/tutorial/loading-and-querying-data-with-hadoop

When you run the program, it defaults to running 3 threads. You can run a different number of threads by supplying an argument to the program. Like this
```
java -cp ./target/concurrent-hive-tester-1.0-SNAPSHOT.jar:/Users/clukasik/tools/apache-jmeter-3.1/lib/ext//hadoop-auth.jar:/Users/clukasik/tools/apache-jmeter-3.1/lib/ext//hadoop-common.jar:/Users/clukasik/tools/apache-jmeter-3.1/lib/ext//hive-jdbc-1.2.1000.2.4.3.0-227-standalone.jar com.hortonworks.examples.hive.DropTest 2
```

Please note that, when running the command, you will need to put the JDBC and Hadoop (common and auth) jars on the classpath.

Adjust and play with the properties file ```src/main/resources/droptest.properties```. There you'll find the connection properties for the DB and the SQL that will be run.

What this program does: 
* Create X number of tables (using "create table as" with the SQL specified in droptest.properties). X is determined by the "num.tables" property in the property file
* After the tables are created:
    * in Y number of threads (specified as the programs only argument), try to drop the table using "if exists" where each thread will be doing the same thing. Since they have "if exists" no command should fail
 
Instructions - RunConcurrent
===========

Create and populate the tables using instructions here: https://hortonworks.com/tutorial/loading-and-querying-data-with-hadoop

Create the ```omniture``` view too, but instead of creating a table for ```webloganalytics```, create it as a view, like this:
```
CREATE view webloganalytics as
SELECT to_date(o.ts) logdate, o.url, o.ip, o.city, upper(o.state) state,
o.country, p.category, CAST(datediff(from_unixtime(unix_timestamp()), from_unixtime(unix_timestamp(u.birth_dt, 'dd-MMM-yy'))) / 365 AS INT) age, u.gender_cd
FROM omniture o
INNER JOIN products p
ON o.url = p.url
LEFT OUTER JOIN users u
ON o.swid = concat('{', u.swid , '}');
```

When you run the program, it defaults to running 3 threads. You can run a different number of threads by supplying an argument to the program. Like this
```
java -cp ./target/concurrent-hive-tester-1.0-SNAPSHOT.jar:/Users/clukasik/tools/apache-jmeter-3.1/lib/ext//hadoop-auth.jar:/Users/clukasik/tools/apache-jmeter-3.1/lib/ext//hadoop-common.jar:/Users/clukasik/tools/apache-jmeter-3.1/lib/ext//hive-jdbc-1.2.1000.2.4.3.0-227-standalone.jar com.hortonworks.examples.hive.RunConcurrent 2
```

Please note that, when running the command, you will need to put the JDBC and Hadoop (common and auth) jars on the classpath.

Adjust and play with the properties file ```src/main/resources/query.properties```. There you'll find the connection properties for the DB and the SQL that will be run.

Important Note: this program will run forever (if there are no exceptions) so remember to kill it before you forget it is running!