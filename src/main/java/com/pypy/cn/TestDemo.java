package com.pypy.cn;

import avro.demain.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestDemo {

    @Test
    public void create(){
        User u1 = new User();
        u1.setAge(23);
        u1.setUsername("tt");

        User u2 = new User("rose",25);
        User u3 = new User().newBuilder().setUsername("kj").setAge(32).build();

        //底层用到clone接口（克隆机制）
        User u4 = new User().newBuilder(u2).setAge(30).build();

        System.out.println(u1);
        System.out.println(u4);
    }

    /**
     * 序列化
     */
    @Test
    public void write() throws IOException {
        User u1 = new User("Ken",1000);
        User u2 = new User("danis",1000);

        DatumWriter<User> dw = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dfw = new DataFileWriter<>(dw);

        //--创建底层输出通道
        dfw.create(u1.getSchema(),new File("1.txt"));
        //--将对象数据写入文件中
        dfw.append(u1);
        dfw.append(u2);
        dfw.close();
    }

    /**
     * 反序列化
     */
    @Test
    public void read() throws IOException {
        DatumReader<User> dr = new SpecificDatumReader<>(User.class);

        DataFileReader<User> dfr = new DataFileReader<User>(new File("1.txt"),dr);

        //通过迭代器找出对象数据
        while(dfr.hasNext()){
            System.out.println(dfr.next());
        }
    }
}
