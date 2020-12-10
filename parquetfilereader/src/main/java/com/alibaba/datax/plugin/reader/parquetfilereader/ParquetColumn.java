package com.alibaba.datax.plugin.reader.parquetfilereader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;

/**
 * 
 * @author jiangdw
 *
 */
public class ParquetColumn {
	
	private Integer index;

	private String name;

	private String type;

	private String format;
	
	private DateFormat dateParse;

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

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
        this.format = format;
        if (StringUtils.isNotBlank(this.format)) {
            this.dateParse = new SimpleDateFormat(this.format);
        }
    }

    public DateFormat getDateFormat() {
        return this.dateParse;
    }

	public DateFormat getDateParse() {
		return dateParse;
	}

	public void setDateParse(DateFormat dateParse) {
		this.dateParse = dateParse;
	}
	
    public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	public String toJSONString() {
        return ParquetColumn.toJSONString(this);
    }

    public static String toJSONString(ParquetColumn columnEntry) {
        return JSON.toJSONString(columnEntry);
    }

}
