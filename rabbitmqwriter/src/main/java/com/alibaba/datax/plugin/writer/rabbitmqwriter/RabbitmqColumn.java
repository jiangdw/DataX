package com.alibaba.datax.plugin.writer.rabbitmqwriter;

/**
 * 
 * @author jiangdw
 *
 */
public class RabbitmqColumn {
	
	private Integer index;

	private String name;

	private String type;

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

}
