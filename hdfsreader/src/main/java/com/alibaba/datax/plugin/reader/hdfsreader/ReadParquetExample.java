package com.alibaba.datax.plugin.reader.hdfsreader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class ReadParquetExample {

	public static void main(String[] args) throws Exception {
//		parquetWriter("/users/jiangdw/desktop/tmp/xxx.par", "/users/jiangdw/desktop/tmp/par.txt");
//		parquetReader("/users/jiangdw/desktop/tmp/part-00000-1e4ba785-c92a-421f-80e4-1afe2a2b4779-c000.snappy.parquet");
		parquetReaderV2("/users/jiangdw/desktop/tmp/part-00000-1e4ba785-c92a-421f-80e4-1afe2a2b4779-c000.snappy.parquet");
	}

	public static void parquetReaderV2(String inPath) throws Exception {
		GroupReadSupport readSupport = new GroupReadSupport();
		Builder<Group> reader = ParquetReader.builder(readSupport, new Path(inPath));
		ParquetReader<Group> build = reader.build();
		Group line = null;
		while ((line = build.read()) != null) {
        	System.out.println(line.toString());

		}
		System.out.println("读取结束");
	}

	public static void parquetReader(String inPath) throws Exception {
		GroupReadSupport readSupport = new GroupReadSupport();
		ParquetReader<Group> reader = new ParquetReader<Group>(new Path(inPath), readSupport);
		Group line = null;
		while ((line = reader.read()) != null) {
			System.out.println(line.toString());
		}
		System.out.println("读取结束");
	}

	static void parquetWriter(String outPath, String inPath) throws IOException {
		MessageType schema = MessageTypeParser.parseMessageType(
				"message Pair {\n"
				+ " required binary city (UTF8);\n"
				+ " required binary ip (UTF8);\n" 
				+ " repeated group time {\n" 
				+ " required int32 ttl;\n"
				+ " required binary ttl2;\n" 
				+ "}\n" + "}");
		GroupFactory factory = new SimpleGroupFactory(schema);
		Path path = new Path(outPath);
		Configuration configuration = new Configuration();
		GroupWriteSupport writeSupport = new GroupWriteSupport();
		writeSupport.setSchema(schema, configuration);
		ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);
		// 把本地文件读取进去，用来生成parquet格式文件
		BufferedReader br = new BufferedReader(new FileReader(new File(inPath)));
		String line = "";
		Random r = new Random();
		while ((line = br.readLine()) != null) {
			String[] strs = line.split("\\s+");
			if (strs.length == 2) {
				Group group = factory.newGroup().append("city", strs[0]).append("ip", strs[1]);
				Group tmpG = group.addGroup("time");
				tmpG.append("ttl", r.nextInt(9) + 1);
				tmpG.append("ttl2", r.nextInt(9) + "_a");
				writer.write(group);
			}
		}
		System.out.println("write end");
		writer.close();
	}

}
