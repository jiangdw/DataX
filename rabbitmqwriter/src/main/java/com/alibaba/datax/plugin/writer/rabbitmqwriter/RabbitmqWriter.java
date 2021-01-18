package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
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

		private Configuration writerSliceConfig;
		
		private Connection connection = null;
		private Channel channel = null;

		private String host;
		private Integer port;
		private String username;
		private String password;
		private String vhost;
		private String queue;
		private String routingKey;
		private String exchange;
		private String fieldDelimiter;
		private Boolean jointColumn;
		private Integer reorderArrayLength;
		private String messagePrefix;
		private String messageSuffix;
		private Integer batchSize;
		
		private List<RabbitmqColumn> columnList = null;
		
		private int count = 0;

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
			fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER, ",");
			jointColumn = this.writerSliceConfig.getBool(Key.JOINT_COLUMN, false);
			reorderArrayLength = this.writerSliceConfig.getInt(Key.RERODER_ARRAY_LENGTH, 0);
			messagePrefix = this.writerSliceConfig.getString(Key.MESSAGE_PREFIX, "");
			messageSuffix = this.writerSliceConfig.getString(Key.MESSAGE_SUFFIX, "");
			batchSize = this.writerSliceConfig.getInt(Key.BATCH_SIZE, 10000);

			columnList = JSONArray.parseArray(this.writerSliceConfig.getString(Key.COLUMN), RabbitmqColumn.class);
			LOG.info("配置：{}  列信息：", JSONObject.toJSONString(writerSliceConfig), this.writerSliceConfig.getString(Key.COLUMN));

			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost(host);
			connectionFactory.setPort(getPort());
			connectionFactory.setUsername(username);
			connectionFactory.setPassword(password);
			connectionFactory.setVirtualHost(vhost);

			try {
				connection = connectionFactory.newConnection();
				channel = connection.createChannel();
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

		@Override
		public void prepare() {
		}

		public int getPort() {
			return null == port ? 5672 : port;
		}

		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			LOG.info("begin do write...");
			List<Record> writerList = new ArrayList<>(batchSize);
			Record record;
			long total = 0;
			while ((record = recordReceiver.getFromReader()) != null) {
				writerList.add(record);
				if (writerList.size() >= batchSize) {
					total += doBatchInsert(writerList);
					writerList.clear();
				}
			}
			if (!writerList.isEmpty()) {
				// LOG.info("本次需要处理的数据大小：{}", writerList.size());
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
				for (Record record : writerList) {
					Map<String, Object> data = new HashMap<>(16);
					StringBuffer sb = new StringBuffer();
					int length = record.getColumnNumber();
					
					// 如果解析的字段需要重组顺序
					Object[] newArray;
					if (reorderArrayLength > 0) {
						newArray = new Object[reorderArrayLength];
					} else {
						newArray = new Object[length];
					}
					
					for (int i = 0; i < length; i++) {
						Column column = record.getColumn(i);
						RabbitmqColumn rabbitmqColumn = columnList.get(i);
						String operation = rabbitmqColumn.getOperation();
						Object rawData = column.getRawData();
						// 对原始数据值进行转换
						if (null != rawData && StringUtils.isNotBlank(operation)) {
							// 判断rawData是否为字符类型数据，如果为字符类型则且不能为空的字符串
							// 或者不为字符类型的
							if ((rawData instanceof String && StringUtils.isNotBlank(rawData.toString()))
								|| !(rawData instanceof String)) {
								Double x = null;
								if (StringUtils.startsWith(operation, "/")) {
									BigDecimal dx = new BigDecimal(String.valueOf(rawData));
									BigDecimal divisor = new BigDecimal(operation.toString().substring(1));
									// 小数点后15位，四舍五入
									x = dx.divide(divisor, 15, RoundingMode.HALF_EVEN).doubleValue();
								}
								if (StringUtils.startsWith(operation, "*")) {
									BigDecimal dx = new BigDecimal(String.valueOf(rawData));
									BigDecimal multiplicand = new BigDecimal(operation.toString().substring(1));
									x = dx.multiply(multiplicand).doubleValue();
								}
								if (StringUtils.startsWith(operation, "-")) {
									BigDecimal dx = new BigDecimal(String.valueOf(rawData));
									BigDecimal subtrahend = new BigDecimal(operation.toString().substring(1));
									x = dx.subtract(subtrahend).doubleValue();
								}
								if (StringUtils.startsWith(operation, "+")) {
									BigDecimal dx = new BigDecimal(String.valueOf(rawData));
									BigDecimal augend = new BigDecimal(operation.toString().substring(1));
									x = dx.add(augend).doubleValue();
								}
								
								// 如果计算出来的x不为null，则将数字类型转为字符串
								if (x != null) {
									NumberFormat nf = NumberFormat.getInstance();
							        nf.setGroupingUsed(false);
									rawData = nf.format(x.doubleValue());
								}
							}
						}
						data.put(rabbitmqColumn.getName(), rawData);
						newArray[rabbitmqColumn.getIndex()] = rawData;
					}
					
					for (Object object : newArray) {
						sb.append(object == null ? "" : object).append(fieldDelimiter);
					}
					
					// 拼接字段以间隔符号隔开
					if (jointColumn) {
						// 给拼接的字符串增加前缀和后缀
						String message = messagePrefix + sb.toString().substring(0, sb.length() - 1) + messageSuffix;
						dataList.add(message);
					} else {
						dataList.add(data);
					}
					
					index++;

					if (index % batchSize == 0) {
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
//			LOG.info("本次批量处理数据数：{}", dataList.size());
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
