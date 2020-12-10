package com.alibaba.datax.plugin.reader.parquetfilereader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Parquet工具类
 * @author jiangdw
 *
 */
public class ParquetReaderUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderUtil.class);

	private ParquetReaderUtil() {

	}

	public static void readFromParquet(ParquetReader.Builder<Group> reader, String context, Configuration readerSliceConfig,
			RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
		String encoding = readerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);

		// warn: no default value '\N'
		String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);

		// warn: Configuration -> List<ColumnEntry> for performance
		// List<Configuration> column = readerSliceConfig
		// .getListConfiguration(Key.COLUMN);
		List<ParquetColumn> column = ParquetReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
		
		// every line logic
		try {
			
			ParquetReader<Group> build = reader.build();
			Group line = null;
			Map<String, String> sourceMap;
			while ((line = build.read()) != null) {
				sourceMap = new HashMap<>();
				for (ParquetColumn parquetColumn : column) {
					try {
						String parquetColumnValue = "";
						String field = parquetColumn.getName();
						String type = parquetColumn.getType();
						
						if ("string".equalsIgnoreCase(type)) {
							parquetColumnValue = line.getString(field, 0);
						} else if ("integer".equalsIgnoreCase(type)) {
							parquetColumnValue = String.valueOf(line.getInteger(field, 0));
						} else if ("long".equalsIgnoreCase(type)) {
							parquetColumnValue = String.valueOf(line.getLong(field, 0));
						} else if ("boolean".equalsIgnoreCase(type)) {
							parquetColumnValue = String.valueOf(line.getBoolean(field, 0));
						} else if ("float".equalsIgnoreCase(type)) {
							parquetColumnValue = String.valueOf(line.getFloat(field, 0));
						} else if ("double".equalsIgnoreCase(type)) {
							parquetColumnValue = String.valueOf(line.getDouble(field, 0));
						} else if ("int96".equalsIgnoreCase(type)) {
							parquetColumnValue = String.valueOf(line.getInt96(field, 0));
						}
						
						sourceMap.put(parquetColumn.getName(), parquetColumnValue);
					} catch (Exception e) {
						sourceMap.put(parquetColumn.getName(), "");
						// 未解析到的属性值设置未空
						LOG.debug("解析文件{}的属性{}出现异常：{}", context, parquetColumn.getName(), e.getLocalizedMessage());
						continue;
					}
				}
	        	ParquetReaderUtil.transportOneRecord(recordSender, column, sourceMap, nullFormat,
						taskPluginCollector);
			}
			
		} catch (UnsupportedEncodingException uee) {
			throw DataXException.asDataXException(ParquetFileReaderErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
					String.format("不支持的编码格式 : [%s]", encoding), uee);
		} catch (FileNotFoundException fnfe) {
			throw DataXException.asDataXException(ParquetFileReaderErrorCode.FILE_NOT_EXISTS,
					String.format("无法找到文件 : [%s]", context), fnfe);
		} catch (IOException ioe) {
			throw DataXException.asDataXException(ParquetFileReaderErrorCode.READ_FILE_IO_ERROR,
					String.format("读取文件错误 : [%s]", context), ioe);
		} catch (Exception e) {
			throw DataXException.asDataXException(ParquetFileReaderErrorCode.RUNTIME_EXCEPTION,
					String.format("运行时异常 : %s", e.getMessage()), e);
		} finally {
//			IOUtils.closeQuietly(reader);
		}
	}

	public static Record transportOneRecord(RecordSender recordSender, List<ParquetColumn> columnConfigs,
			Map<String, String> sourceMap, String nullFormat, TaskPluginCollector taskPluginCollector) {
		Record record = recordSender.createRecord();
		Column columnGenerated = null;

		// 创建都为String类型column的record
		if (null == columnConfigs || columnConfigs.size() == 0) {
			String columnValue;
			for (Map.Entry<String, String> entry : sourceMap.entrySet()) {
				columnValue = entry.getValue();
				// not equalsIgnoreCase, it's all ok if nullFormat is null
				if (columnValue.equals(nullFormat)) {
					columnGenerated = new StringColumn(null);
				} else {
					columnGenerated = new StringColumn(columnValue);
				}
				record.addColumn(columnGenerated);
			}
			recordSender.sendToWriter(record);
		} else {
			try {
				for (ParquetColumn columnConfig : columnConfigs) {
					String columnType = columnConfig.getType();
					String columnName = columnConfig.getName();

					String columnValue = null;

					if (null == columnType && null == columnName) {
						throw DataXException.asDataXException(ParquetFileReaderErrorCode.NO_NAME_TYPE,
								"由于您配置了type, 则需要配置 name");
					}

					columnValue = sourceMap.get(columnName);
					
					Type type = Type.valueOf(columnType.toUpperCase());
					// it's all ok if nullFormat is null
					if (columnValue.equals(nullFormat)) {
						columnValue = null;
					}
					switch (type) {
					case STRING:
						columnGenerated = new StringColumn(columnValue);
						break;
					case INTEGER:
						try {
							columnGenerated = new LongColumn(columnValue);
						} catch (Exception e) {
							throw new IllegalArgumentException(
									String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "INTEGER"));
						}
						break;
					case LONG:
						try {
							columnGenerated = new LongColumn(columnValue);
						} catch (Exception e) {
							throw new IllegalArgumentException(
									String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "LONG"));
						}
						break;
					case DOUBLE:
						try {
							columnGenerated = new DoubleColumn(columnValue);
						} catch (Exception e) {
							throw new IllegalArgumentException(
									String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "DOUBLE"));
						}
						break;
					case BOOLEAN:
						try {
							columnGenerated = new BoolColumn(columnValue);
						} catch (Exception e) {
							throw new IllegalArgumentException(
									String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "BOOLEAN"));
						}

						break;
					case DATE:
						try {
							if (columnValue == null) {
								Date date = null;
								columnGenerated = new DateColumn(date);
							} else {
								String formatString = columnConfig.getFormat();
								// if (null != formatString) {
								if (StringUtils.isNotBlank(formatString)) {
									// 用户自己配置的格式转换, 脏数据行为出现变化
									DateFormat format = columnConfig.getDateFormat();
									columnGenerated = new DateColumn(format.parse(columnValue));
								} else {
									// 框架尝试转换
									columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
								}
							}
						} catch (Exception e) {
							throw new IllegalArgumentException(
									String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "DATE"));
						}
						break;
					default:
						String errorMessage = String.format("您配置的列类型暂不支持 : [%s]", columnType);
						LOG.error(errorMessage);
						throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
								errorMessage);
					}

					record.addColumn(columnGenerated);

				}
				recordSender.sendToWriter(record);
			} catch (IllegalArgumentException iae) {
				taskPluginCollector.collectDirtyRecord(record, iae.getMessage());
			} catch (IndexOutOfBoundsException ioe) {
				taskPluginCollector.collectDirtyRecord(record, ioe.getMessage());
			} catch (Exception e) {
				if (e instanceof DataXException) {
					throw (DataXException) e;
				}
				// 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
				taskPluginCollector.collectDirtyRecord(record, e.getMessage());
			}
		}

		return record;
	}

	public static List<ParquetColumn> getListColumnEntry(Configuration configuration, final String path) {
		List<JSONObject> lists = configuration.getList(path, JSONObject.class);
		if (lists == null) {
			return null;
		}
		List<ParquetColumn> result = new ArrayList<>();
		for (final JSONObject object : lists) {
			result.add(JSON.parseObject(object.toJSONString(), ParquetColumn.class));
		}
		return result;
	}

	private enum Type {
		STRING, INTEGER, LONG, BOOLEAN, DOUBLE, DATE;
	}

	/**
	 * check parameter:encoding, compress, filedDelimiter
	 */
	public static void validateParameter(Configuration readerConfiguration) {

		// encoding check
		validateEncoding(readerConfiguration);

		// only support compress types
		validateCompress(readerConfiguration);

		// column: 1. index type 2.value type 3.when type is Date, may have format
		validateColumn(readerConfiguration);

	}

	public static void validateEncoding(Configuration readerConfiguration) {
		// encoding check
		String encoding = readerConfiguration.getString(
				com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
				com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
		try {
			encoding = encoding.trim();
			readerConfiguration.set(Key.ENCODING, encoding);
			Charsets.toCharset(encoding);
		} catch (UnsupportedCharsetException uce) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
		} catch (Exception e) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.CONFIG_INVALID_EXCEPTION,
					String.format("编码配置异常, 请联系我们: %s", e.getMessage()), e);
		}
	}

	public static void validateCompress(Configuration readerConfiguration) {
		String compress = readerConfiguration
				.getUnnecessaryValue(Key.COMPRESS, null, null);
		if (StringUtils.isNotBlank(compress)) {
			compress = compress.toLowerCase().trim();
			boolean compressTag = "gzip".equals(compress) || "bzip2".equals(compress) || "zip".equals(compress)
					|| "lzo".equals(compress) || "lzo_deflate".equals(compress) || "hadoop-snappy".equals(compress)
					|| "framing-snappy".equals(compress);
			if (!compressTag) {
				throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
						String.format("仅支持 gzip, bzip2, zip, lzo, lzo_deflate, hadoop-snappy, framing-snappy "
								+ "文件压缩格式, 不支持您配置的文件压缩格式: [%s]", compress));
			}
		} else {
			// 用户可能配置的是 compress:"",空字符串,需要将compress设置为null
			compress = null;
		}
		readerConfiguration.set(Key.COMPRESS, compress);

	}

	public static void validateColumn(Configuration readerConfiguration) {
		// column: 1. index type 2.value type 3.when type is Date, may have
		// format
		List<Configuration> columns = readerConfiguration
				.getListConfiguration(Key.COLUMN);
		if (null == columns || columns.size() == 0) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.REQUIRED_VALUE, "您需要指定 columns");
		}
		// handle ["*"]
		if (null != columns && 1 == columns.size()) {
			String columnsInStr = columns.get(0).toString();
			if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
				readerConfiguration.set(Key.COLUMN, null);
				columns = null;
			}
		}

		if (null != columns && columns.size() != 0) {
			for (Configuration eachColumnConf : columns) {
				eachColumnConf.getNecessaryValue(Key.TYPE,
						ParquetFileReaderErrorCode.REQUIRED_NAME);
				String columnName = eachColumnConf
						.getString(Key.NAME);
				String columnType = eachColumnConf
						.getString(Key.TYPE);

				if (null == columnName && null == columnType) {
					throw DataXException.asDataXException(ParquetFileReaderErrorCode.NO_NAME_TYPE,
							"由于您配置了type, 则需要配置 name");
				}

			}
		}
	}

	/**
	 *
	 * @Title: getRegexPathParent @Description: 获取正则表达式目录的父目录 @param @param
	 *         regexPath @param @return @return String @throws
	 */
	public static String getRegexPathParent(String regexPath) {
		int endMark;
		for (endMark = 0; endMark < regexPath.length(); endMark++) {
			if ('*' != regexPath.charAt(endMark) && '?' != regexPath.charAt(endMark)) {
				continue;
			} else {
				break;
			}
		}
		int lastDirSeparator = regexPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
		String parentPath = regexPath.substring(0, lastDirSeparator + 1);

		return parentPath;
	}

	/**
	 *
	 * @Title: getRegexPathParentPath @Description:
	 *         获取含有通配符路径的父目录，目前只支持在最后一级目录使用通配符*或者?. (API
	 *         jcraft.jsch.ChannelSftp.ls(String path)函数限制)
	 *         http://epaul.github.io/jsch-documentation/javadoc/ @param @param
	 *         regexPath @param @return @return String @throws
	 */
	public static String getRegexPathParentPath(String regexPath) {
		int lastDirSeparator = regexPath.lastIndexOf(IOUtils.DIR_SEPARATOR);
		String parentPath = "";
		parentPath = regexPath.substring(0, lastDirSeparator + 1);
		if (parentPath.contains("*") || parentPath.contains("?")) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("配置项目path中：[%s]不合法，目前只支持在最后一级目录使用通配符*或者?", regexPath));
		}
		return parentPath;
	}

}
