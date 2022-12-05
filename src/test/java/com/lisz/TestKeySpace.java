package com.lisz;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.DropKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestKeySpace {
    private Cluster cluster;
    private Session session;
    /**
     * 连接Cassandra服务端
     */
    @Before
    public void init(){
        //服务器的地址
        String host = "192.168.1.24";
        int port = 9042;
        //连接服务端获取会话
        cluster = Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                .build();
        session = cluster.connect();
    }

    @After
    public void close() {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    /**
     * 查询所有的键空间
     */
    @Test
    public void findKeySpace(){
        List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
        keyspaces.forEach(System.out::println);
    }

    @Test
    public void createKeySpace(){
        session.execute("create KEYSPACE abc WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        findKeySpace();
    }

    @Test
    public void createKeySpace2(){
        Map<String, Object> replication = new HashMap<>();
        replication.put("class", "SimpleStrategy");
        replication.put("replication_factor", 2);
        KeyspaceOptions options = SchemaBuilder.createKeyspace("cities")
                .ifNotExists()
                .with()
                .replication(replication);
        session.execute(options);
        findKeySpace();
    }

    @Test
    public void deleteKeySpace(){
        DropKeyspace cities = SchemaBuilder.dropKeyspace("cities").ifExists();
        session.execute(cities);
        findKeySpace();
    }

    /**
     * 修改键空间
     */
    @Test
    public void alterKeySpace(){
        Map<String, Object> replication = new HashMap<>();
        replication.put("class", "SimpleStrategy");
        replication.put("replication_factor", 2);
        KeyspaceOptions options = SchemaBuilder.alterKeyspace("abc")
                .with()
                .replication(replication);
        session.execute(options);
        findKeySpace();
    }
}
