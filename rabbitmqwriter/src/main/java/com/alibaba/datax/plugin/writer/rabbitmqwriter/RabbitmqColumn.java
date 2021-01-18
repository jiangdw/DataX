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

	/**
	 * 字段名称
	 */
	private String name;

	/**
	 * 输入字段类型：string,date,int,double,float
	 */
	private String type;
	
	/**
	 * 操作：/100，*100，+100，-100
	 */
	private String operation;
	
	/**
	 * 格式化对象的格式，如对日期字符串转换为日期类型，则需要指定输入内容的格式
	 */
	private String pattern;
	
	/**
	 * 转换日期的时区：+8，+0，-9
	 */
	private String timezone;
	
	/**
	 * 转换为的格式：timestamp，time，date，
	 */
	private String translateTo;

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

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public String getTimezone() {
		return timezone;
	}

	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}

	public String getTranslateTo() {
		return translateTo;
	}

	public void setTranslateTo(String translateTo) {
		this.translateTo = translateTo;
	}

}
