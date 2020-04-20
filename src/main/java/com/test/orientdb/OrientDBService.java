package com.test.orientdb;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.nio.charset.StandardCharsets;
@Service
public class OrientDBService {

    OServer server;
    ODatabaseDocumentInternal database;

    final String dbName = "TestDB";
    @Value("${server.dbName}")
    String nodeName;
    OCommandOutputListener listener = new OCommandOutputListener() {
        @Override
        public void onMessage(String iText) {
            System.out.print(iText);
        }
    };
    private String escapePath(String path) {
        return StringEscapeUtils.escapeJava(StringEscapeUtils.escapeXml(new File(path).getAbsolutePath()));
    }
    private InputStream getOrientServerConfig(String nodeName, String basePath) throws IOException {
        InputStream configIns = OrientDBService.class.getClassLoader().getResourceAsStream("config/orientdb-server-config.xml");
        StringWriter writer = new StringWriter();
        IOUtils.copy(configIns, writer, StandardCharsets.UTF_8);
        String configString = writer.toString();
        configString = configString.replaceAll("%PLUGIN_DIRECTORY%", "orient-plugins");
        configString = configString.replaceAll("%CONSOLE_LOG_LEVEL%", "finest");
        configString = configString.replaceAll("%FILE_LOG_LEVEL%", "fine");
        configString = configString.replaceAll("%DB_PATH%", "plocal:" + escapePath(basePath + "/storage"));
        configString = configString.replaceAll("%NODENAME%", nodeName);
        configString = configString.replaceAll("%DB_PARENT_PATH%", escapePath(basePath));
        InputStream stream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));
        return stream;
    }
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
    @PostConstruct
    public void init() throws Exception {
        server = OServerMain.create();
        //TODO: System.setProperty("ORIENTDB_HOME", orientdbHome);
        //TODO: configurable nodename
        server.startup(getOrientServerConfig(nodeName, "data/"+nodeName));
        server.activate();
        //TODO: configurable db name
        if (!server.existsDatabase(dbName)){
            // create database
            server.createDatabase(dbName, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
            database = server.openDatabase(dbName, "root", "root");
            // import schema
            /*ODatabaseImport dbImport = new ODatabaseImport(database, "config/schema.gz", listener);
            dbImport.importDatabase();
            dbImport.close();*/
            database.createClass("TEST", "V");
            database.createClass("TEST_EDGE", "E");

            //insert test object
            //TODO: remove later, only for testing
            OResultSet result = database.command("create vertex TEST set name='root'");
            result.next().getProperty("@rid").toString();
            result.close();
        } else {
            database = server.openDatabase(dbName, "root", "root");
        }
    }

    @PreDestroy
    public void shutdown() {
        server.shutdown();
    }

    public void doSomeFancyStuff() {
        ODatabaseRecordThreadLocal.instance().set(database);
        int maxRetries = 10;

        OResultSet existingRoot = database.command("select from TEST where name='root'");
        OVertex root = existingRoot.next().getVertex().get();
        for (int retry = 0; retry < maxRetries; ++retry) {            database.begin();

            try {

                System.out.println("Doing some fancy stuff!");
                OResultSet newVertex = database.command("create vertex TEST");

                database.command("create edge TEST_EDGE from " + newVertex.next().getProperty("@rid").toString() + " to " + root.getProperty("@rid"));
                newVertex.close();
                existingRoot.close();
                database.commit();
                return;
            } catch (OConcurrentModificationException e) {
                root.reload();
                System.out.println("ConcurrentModificationException occured, retrying...("+retry+")");
            }
        }
        database.rollback();
    }
}
