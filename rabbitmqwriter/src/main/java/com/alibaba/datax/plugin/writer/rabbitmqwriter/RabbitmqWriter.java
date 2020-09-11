package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Created by jiangdw on 2020-09-09.
 */
public class RabbitmqWriter extends Writer {

	private final static String WRITE_COLUMNS = "columns";

	private final static int BATCH_SIZE = 1000;

	public static class Job extends Writer.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originalConfig = null;

		@Override
		public void preCheck() {
			super.preCheck();
		}

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
		}

		@Override
		public void prepare() {
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			LOG.info("begin do split...");
			List<Configuration> writerSplitConfigs = new ArrayList<>();
			for (int i = 0; i < mandatoryNumber; i++) {
				writerSplitConfigs.add(this.originalConfig);
			}
			LOG.info("end do split.");
			return writerSplitConfigs;
		}

	}

	public static class Task extends Writer.Task {
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);

		private int count = 0;

		private Configuration writerSliceConfig;

		private List<RabbitmqColumn> columnList = null;

		private String queue;
		private String routingKey;
		private String exchange;

		private Connection connection = null;
		private Channel channel = null;

		private String host;
		private Integer port;
		private String username;
		private String password;
		private String vhost;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();
			queue = this.writerSliceConfig.getString(Key.QUEUE, "datax_queue");
			routingKey = this.writerSliceConfig.getString(Key.ROUTINGKEY, "datax.message");
			exchange = this.writerSliceConfig.getString(Key.EXCHANGE, "datax_exchange");
			host = this.writerSliceConfig.getString(Key.HOST, "127.0.0.1");
			port = this.writerSliceConfig.getInt(Key.PORT, 5672);
			username = this.writerSliceConfig.getString(Key.USERNAME, "admin");
			password = this.writerSliceConfig.getString(Key.PASSWORD, "");
			vhost = this.writerSliceConfig.getString(Key.VHOST, "/");

			columnList = JSONArray.parseArray(this.writerSliceConfig.getString(WRITE_COLUMNS), RabbitmqColumn.class);

			LOG.info("配置：{}  列信息：", JSONObject.toJSONString(writerSliceConfig),
					this.writerSliceConfig.getString(WRITE_COLUMNS));
		}

		@Override
		public void prepare() {
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost(host);
			connectionFactory.setPort(getPort());
			connectionFactory.setUsername(username);
			connectionFactory.setPassword(password);
			connectionFactory.setVirtualHost(vhost);

			try {
				Connection connection = connectionFactory.newConnection();
				Channel channel = connection.createChannel();
				// 创建一个type=topic 持久化的 非自动删除的交换器
				channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);
				// 创建一个持久化 排他的 非自动删除的队列
				channel.queueDeclare(queue, true, false, false, null);
				// 将交换器与队列通过路由键绑定
				channel.queueBind(queue, exchange, routingKey);
			} catch (Exception e) {
				LOG.error("Rabbit mq 建立连接失败：" + e.getLocalizedMessage());
			}
			check(connection, channel);
		}

		public int getPort() {
			return null == port ? 5672 : port;
		}

		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			LOG.info("begin do write...");
			List<Record> writerList = new ArrayList<>(BATCH_SIZE);
			Record record;
			long total = 0;
			while ((record = recordReceiver.getFromReader()) != null) {
				writerList.add(record);
				if (writerList.size() >= BATCH_SIZE) {
					total += doBatchInsert(writerList);
					writerList.clear();
				}
			}
			if (!writerList.isEmpty()) {
				LOG.info("本次需要处理的数据大小：{}", writerList.size());
				total += doBatchInsert(writerList);
				writerList.clear();
			}
			String msg = String.format("task end, write size :%d ，msg count：%d", total, count);
			getTaskPluginCollector().collectMessage("writesize", String.valueOf(total));
			LOG.info(msg);
			LOG.info("end do write");
		}

		private long doBatchInsert(final List<Record> writerList) {
			int index = 0;
			try {
				List<Object> dataList = new ArrayList<>();
				LOG.info("本次批量处理数据数：{}，当前第", writerList.size());
				for (Record record : writerList) {
					Map<String, Object> data = new HashMap<>(16);
					int length = record.getColumnNumber();
					if (length > 1) {
						for (int i = 0; i < length; i++) {
							Column column = record.getColumn(i);
							data.put(columnList.get(i).getName(), column.getRawData());
						}
						dataList.add(data);
					} else {
						Column column = record.getColumn(0);
						dataList.add(column.getRawData());
					}
					
					index++;

					if (index % 10 == 0) {
						sendMessage(dataList);
					}
				}

				if (!dataList.isEmpty()) {
					sendMessage(dataList);
				}
			} catch (Exception e) {
				LOG.error(e.getMessage());
				throw DataXException.asDataXException(RabbitmqWriterErrorCode.EXECUTE_ERROR, e);
			}
			return index;
		}

		/**
		 * 发送消息到mq
		 * 
		 * @param dataList 数据列表
		 * @throws IOException
		 */
		private void sendMessage(List<Object> dataList) throws IOException {
			AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
					// 发送消息设置发送模式deliveryMode=2代表持久化消息
					// org.springframework.amqp.rabbit.core.RabbitTemplate默认情况下发送模式为deliveryMode=2
					// org.springframework.amqp.core.MessageProperties
					.deliveryMode(2)
					.contentEncoding("UTF-8").build();
			String message = JSONObject.toJSONString(dataList);
			channel.basicPublish(exchange, routingKey, properties, message.getBytes("UTF-8"));
			dataList.clear();
			count++;
		}

		@Override
		public void post() {
			super.post();
		}

		@Override
		public void destroy() {
			try {
				if (channel != null && channel.isOpen()) {
					channel.close();
				}
				if (connection != null && connection.isOpen()) {
					connection.close();
				}
			} catch (IOException e) {
				LOG.error("关闭rabbitmq连接异常：{}", e.getMessage());
			} catch (TimeoutException e) {
				LOG.error("关闭rabbitmq连接超时：{}", e.getMessage());
			}
		}

		/**
		 * 校验错误
		 * 
		 * @param connection Connection
		 * @param channel    Channel
		 */
		private void check(Connection connection, Channel channel) {
			if (connection == null) {
				throw DataXException.asDataXException(RabbitmqWriterErrorCode.CONNECT_MQ_FAIL, "获取Connection失败！");
			}
			if (channel == null) {
				throw DataXException.asDataXException(RabbitmqWriterErrorCode.CONNECT_MQ_FAIL, "获取channel失败！");
			}
		}
	}
}
