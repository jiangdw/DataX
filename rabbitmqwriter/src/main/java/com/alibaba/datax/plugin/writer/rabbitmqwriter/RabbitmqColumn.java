package com.alibaba.datax.plugin.writer.rabbitmqwriter;

/**
 * 
 * @author jiangdw
 *
 */
public class RabbitmqColumn {
	
	/**
	 * 解析的字符串中的位置索引
	 */
	private Integer index;

	private String name;

	private String type;
	
	/**
	 * 操作：/100，*100，+100，-100
	 */
	private String operation;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

}
