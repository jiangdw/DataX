package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Rabbitmq工具类
 * @author jiangdw
 *
 */
public class RabbitmqUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqUtil.class);

	private RabbitmqUtil() {
	}

	/**
	 * 获取连接
	 * @param conf Configuration
	 * @return Connection
	 */
	public Connection getConnection(Configuration conf) {
		// 创建一个新的连接
		Connection connection = null;
		try {
			
			String queue = conf.getString("queue");
			String routingKey = conf.getString("routingKey");
			String exchange = conf.getString("exchange");

			String host = conf.getString(Key.HOST, "127.0.0.1");
			Integer port = conf.getInt(Key.PORT, 5672);
			String username = conf.getString(Key.USERNAME, "admin");
			String password = conf.getString(Key.PASSWORD, "");
			String vhost = conf.getString(Key.VHOST, "/");
			
			// 创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(conf.getString(Key.HOST, "127.0.0.1"));
			factory.setPort(conf.getInt(Key.PORT, 5672));
			factory.setUsername(conf.getString(Key.USERNAME, "admin"));
			factory.setPassword(conf.getString(Key.PASSWORD, ""));
			factory.setConnectionTimeout(3000);
			factory.setVirtualHost(conf.getString(Key.VHOST, "/"));
			connection = factory.newConnection();
			LOGGER.info("Rabbitmq配置：host: {}, port: {}, username: {}, password: {}, vhost: {}, queue: {}, exchange: {}, routingKey: {}",
					conf.getString("host"), conf.getString("port"), conf.getString("username"),
					conf.getString("password"), conf.getString("vhost"), conf.getString("queue"),
					conf.getString("exchange"), conf.getString("routingKey"));
		} catch (IOException e) {
			LOGGER.error("获取rabbitmq连接异常：{}", e.getMessage());
		} catch (TimeoutException e) {
			LOGGER.error("获取rabbitmq连接超时：{}", e.getMessage());
		}
		return connection;
	}

	/**
	 * 获取Channel
	 * @param conn Connection
	 * @param conf Configuration
	 * @return Channel
	 */
	public Channel getChannel(Connection conn, Configuration conf) {
		// 创建一个新的连接
		Channel channel = null;
		try {
			channel = conn.createChannel();
			channel.queueBind(conf.getString("queue"), conf.getString("exchange"), conf.getString("routingKey"));
		} catch (IOException e) {
			LOGGER.error("获取rabbitmq连接异常：{}", e.getMessage());
		}
		return channel;
	}

	/**
	 * 关闭连接
	 * @param conn Connection
	 * @param channel Channel
	 */
	public void closeConnection(Connection conn, Channel channel) {
		try {
			if (channel != null && channel.isOpen()) {
				channel.close();
			}
			if (conn != null && conn.isOpen()) {
				conn.close();
			}
		} catch (IOException e) {
			LOGGER.error("关闭rabbitmq连接异常：{}", e.getMessage());
		} catch (TimeoutException e) {
			LOGGER.error("关闭rabbitmq连接超时：{}", e.getMessage());
		}
	}

	public static RabbitmqUtil getInstance() {
		return RabbitmqUtilHandler.instance;
	}

	private static class RabbitmqUtilHandler {
		public static RabbitmqUtil instance = new RabbitmqUtil();
	}

}
