package com.alibaba.datax.plugin.writer.rabbitmqwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by jiangdw on 2020-09-09.
 */
public enum RabbitmqWriterErrorCode implements ErrorCode {

	BAD_CONFIG_VALUE("RabbitmqWriter-00", "您配置的值不合法."), 
	EXECUTE_ERROR("RabbitmqWriter-01", "执行时失败."),
	CONNECT_MQ_FAIL("RabbitmqWriter-02", "连接Rabbitmq失败");

	private final String code;
	private final String description;

	private RabbitmqWriterErrorCode(String code, String description) {
		this.code = code;
		this.description = description;
	}

	@Override
	public String getCode() {
		return this.code;
	}

	@Override
	public String getDescription() {
		return this.description;
	}

	@Override
	public String toString() {
		return String.format("Code:[%s], Description:[%s].", this.code, this.description);
	}

}
