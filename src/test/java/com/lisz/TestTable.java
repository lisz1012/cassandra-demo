package com.lisz;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.lisz.model.Student;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TestTable {
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

    @Test
    public void testCreateTable() {
        Create create = SchemaBuilder.createTable("school", "student").ifNotExists()
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .addColumn("age", DataType.cint())
                .addColumn("address", DataType.text())
                .addColumn("gender", DataType.cint())
                .addColumn("interest", DataType.set(DataType.text()))
                .addColumn("phone", DataType.list(DataType.text()))
                .addColumn("education", DataType.map(DataType.text(), DataType.text()));
        session.execute(create);
    }

    @Test
    public void testAlterTable() {
        SchemaStatement statement = SchemaBuilder.alterTable("abc", "student")
                .addColumn("email").type(DataType.text());
        session.execute(statement);
    }

//    @Test // 不允许
//    public void testAlterColumn() {
//        SchemaStatement statement = SchemaBuilder.alterTable("abc", "student")
//                .alterColumn("email")
//                .type(DataType.set(DataType.text()));
//        session.execute(statement);
//    }

    @Test
    public void testDeleteColumn() {
        SchemaStatement statement = SchemaBuilder.alterTable("abc", "student")
                .dropColumn("email");
        session.execute(statement);
    }

    @Test
    public void testDropTable() {
        Drop drop = SchemaBuilder.dropTable("school", "student").ifExists();
        session.execute(drop);
    }

    @Test
    public void testInsertByCQL(){
        String cql = "INSERT INTO school.student (id,address,age,gender,name,interest, phone,education) VALUES " +
                "(1016,'朝阳路19号',17,2,'Cary',{'看书', '电影'},['020-66666666','13666666666']," +
                "{'小学' :'城市第五小学','中学':'城市第五中学'});";
        session.execute(cql);
    }

    @Test
    public void testInsertByMapper(){
        Set<String> interest = new HashSet<>();
        interest.add("足球");
        interest.add("电影");
        List<String> phone = new ArrayList<>();
        phone.add("020-222222");
        phone.add("010-888888");
        Map<String, String> education = new HashMap<>();
        education.put("小学", "第3小学");
        education.put("大学", "北大");
        Student student = new Student(1012L, "朝阳路21号", 16, "jerry", 0,
                phone, interest, education);
        Mapper mapper = new MappingManager(session).mapper(Student.class);
        mapper.save(student);
    }

    /**
     * 查询所有数据
     */
    @Test
    public void findAll(){
        ResultSet rs = session.execute(QueryBuilder.select().from("school", "student"));
        Mapper<Student> mapper = new MappingManager(session).mapper(Student.class);
        Result<Student> map = mapper.map(rs);
        List<Student> students = map.all();
        students.forEach(System.out::println);
    }

    /**
     * 根据主键来查询
     */
    @Test
    public void testFindById(){
        ResultSet rs = session.execute(QueryBuilder.select().from("school", "student")
                .where(QueryBuilder.eq("id", 1012)));
        Mapper<Student> mapper = new MappingManager((session)).mapper(Student.class);
        Student student = mapper.map(rs).one();
        System.out.println(student);
    }

    /**
     * 按照ID删除
     */
    @Test
    public void testDeleteById(){
        session.execute(QueryBuilder.delete().from("school", "student").where(QueryBuilder.eq("id", 1012)));
    }

    /**
     * 按照ID删除
     */
    @Test
    public void testDeleteById2(){
        Mapper<Student> mapper = new MappingManager(session).mapper(Student.class);
        Long id = 1012L;
        mapper.delete(id);
    }

    /**
     * 创建普通索引
     */
    @Test
    public void testCreateIndex(){
        SchemaStatement statement = SchemaBuilder.createIndex("name_idx")
                .onTable("school", "student")
                .andColumn("name");
        session.execute(statement);
    }

    /**
     * 创建map key索引
     */
    @Test
    public void testCreateKeysIndex(){
        SchemaStatement statement = SchemaBuilder.createIndex("education_idx")
                .onTable("school", "student")
                .andKeysOfColumn("education");
        session.execute(statement);
    }

    /**
     * 删除索引
     */
    @Test
    public void testDropIndex(){
        Drop drop = SchemaBuilder.dropIndex("school", "name_idx").ifExists();
        session.execute(drop);
        drop = SchemaBuilder.dropIndex("school", "education_idx").ifExists();
        session.execute(drop);
    }

    /**
     * 预编译，server端先缓存带占位符的query，然后返回一个id，随后客户端仅仅把id和values传给服务端
     * 服务端去执行。只缓存一次即可，缓存多次会报错。数据量小不值当的，数据量大的时候会大幅度提升效率，
     * 所以配合batch使用，减少边际效应
     */
    @Test
    public void testBatchPrepare(){
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement prepare = session.prepare("INSERT INTO school.student " +
                " (id,address,age,name,gender,phone,interest,education) VALUES " +
                " (?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedId preparedId = prepare.getPreparedId();
        for (int i = 0; i < 10; i++) {
            Set<String> interest = new HashSet<>();
            interest.add("足球");
            interest.add("电影");
            List<String> phone = new ArrayList<>();
            phone.add("020-222222");
            phone.add("010-888888");
            Map<String, String> education = new HashMap<>();
            education.put("小学", "第" + (i + 1) + "小学");
            education.put("大学", "北大");
            Student student = new Student(Long.valueOf(i), "朝阳路" + (22 + i) + "号", 16+i,
                    "jerry" + i, 0,
                    phone, interest, education);
            BoundStatement boundStatement = prepare.bind(student.getId(), student.getAddress(), student.getAge(), student.getName(),
                    student.getGender(), student.getPhone(), student.getInterest(), student.getEducation());
            batchStatement.add(boundStatement);
        }
        session.execute(batchStatement);
        batchStatement.clear();
    }
}
