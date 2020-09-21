package com.alibaba.datax.plugin.reader.parquetfilereader;

import java.io.File;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Sets;

/**
 * Created by jiangdw on 2020-09-16.
 */
public class ParquetFileReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originConfig = null;

		private List<String> path = null;

		private List<String> sourceFiles;

		private Map<String, Pattern> pattern;

		private Map<String, Boolean> isRegexPath;

		@Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			this.pattern = new HashMap<String, Pattern>();
			this.isRegexPath = new HashMap<String, Boolean>();
			this.validateParameter();
		}

		private void validateParameter() {
			// Compatible with the old version, path is a string before
			String pathInString = this.originConfig.getNecessaryValue(Key.PATH,
					ParquetFileReaderErrorCode.REQUIRED_NAME);
			if (StringUtils.isBlank(pathInString)) {
				throw DataXException.asDataXException(
						ParquetFileReaderErrorCode.REQUIRED_NAME,
						"您需要指定待读取的源目录或文件");
			}
			if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
				path = new ArrayList<String>();
				path.add(pathInString);
			} else {
				path = this.originConfig.getList(Key.PATH, String.class);
				if (null == path || path.size() == 0) {
					throw DataXException.asDataXException(
							ParquetFileReaderErrorCode.REQUIRED_NAME,
							"您需要指定待读取的源目录或文件");
				}
			}

			String encoding = this.originConfig
					.getString(
							com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
							com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
			if (StringUtils.isBlank(encoding)) {
                this.originConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                                com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
			} else {
				try {
					encoding = encoding.trim();
					this.originConfig
							.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
									encoding);
					Charsets.toCharset(encoding);
				} catch (UnsupportedCharsetException uce) {
					throw DataXException.asDataXException(
							ParquetFileReaderErrorCode.ILLEGAL_NAME,
							String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
				} catch (Exception e) {
					throw DataXException.asDataXException(
							ParquetFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
							String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
							e);
				}
			}

			// column: 1. index type 2.value type 3.when type is Date, may have
			// format
			List<Configuration> columns = this.originConfig
					.getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
			// handle ["*"]
			if (null != columns && 1 == columns.size()) {
				String columnsInStr = columns.get(0).toString();
				if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
					this.originConfig
							.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN,
									null);
					columns = null;
				}
			}

			if (null != columns && columns.size() != 0) {
				for (Configuration eachColumnConf : columns) {
					eachColumnConf
							.getNecessaryValue(
									com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE,
									ParquetFileReaderErrorCode.REQUIRED_NAME);
					String columnType = eachColumnConf
							.getString(Key.TYPE);
					String columnName = eachColumnConf
							.getString(Key.NAME);

					if (null == columnType && null == columnName) {
						throw DataXException.asDataXException(
								ParquetFileReaderErrorCode.NO_NAME_TYPE,
								"您明确的配置列信息,但未填写相应的name,type");
					}

				}
			}

			// only support compress types
			String compress = this.originConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS);
			if (StringUtils.isBlank(compress)) {
				this.originConfig
						.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,
								null);
			} else {
				Set<String> supportedCompress = Sets
						.newHashSet("gzip", "bzip2", "zip");
				compress = compress.toLowerCase().trim();
				if (!supportedCompress.contains(compress)) {
					throw DataXException
							.asDataXException(
									ParquetFileReaderErrorCode.ILLEGAL_NAME,
									String.format(
											"仅支持 gzip, bzip2, zip 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
											compress));
				}
				this.originConfig
						.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,
								compress);
			}

		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			// warn:make sure this regex string
			// warn:no need trim
			for (String eachPath : this.path) {
				String regexString = eachPath.replace("*", ".*").replace("?", ".?");
				Pattern patt = Pattern.compile(regexString);
				this.pattern.put(eachPath, patt);
				this.sourceFiles = this.buildSourceTargets();
			}

			LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

		// warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.debug("split() begin...");
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			// warn:每个slice拖且仅拖一个文件,
			// int splitNumber = adviceNumber;
			int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(
                        ParquetFileReaderErrorCode.EMPTY_DIR_EXCEPTION, String
                                .format("未能找到待读取的文件,请确认您的配置项path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

			List<List<String>> splitedSourceFiles = this.splitSourceFiles(
					this.sourceFiles, splitNumber);
			for (List<String> files : splitedSourceFiles) {
				Configuration splitedConfig = this.originConfig.clone();
				splitedConfig.set(Constant.SOURCE_FILES, files);
				readerSplitConfigs.add(splitedConfig);
			}
			LOG.debug("split() ok and end...");
			return readerSplitConfigs;
		}

		// validate the path, path must be a absolute path
		private List<String> buildSourceTargets() {
			// for eath path
			Set<String> toBeReadFiles = new HashSet<String>();
			for (String eachPath : this.path) {
				int endMark;
				for (endMark = 0; endMark < eachPath.length(); endMark++) {
					if ('*' != eachPath.charAt(endMark)
							&& '?' != eachPath.charAt(endMark)) {
						continue;
					} else {
						this.isRegexPath.put(eachPath, true);
						break;
					}
				}

				String parentDirectory;
				if (BooleanUtils.isTrue(this.isRegexPath.get(eachPath))) {
					int lastDirSeparator = eachPath.substring(0, endMark)
							.lastIndexOf(IOUtils.DIR_SEPARATOR);
					parentDirectory = eachPath.substring(0,
							lastDirSeparator + 1);
				} else {
					this.isRegexPath.put(eachPath, false);
					parentDirectory = eachPath;
				}
				this.buildSourceTargetsEathPath(eachPath, parentDirectory,
						toBeReadFiles);
			}
			return Arrays.asList(toBeReadFiles.toArray(new String[0]));
		}

		private void buildSourceTargetsEathPath(String regexPath,
				String parentDirectory, Set<String> toBeReadFiles) {
			// 检测目录是否存在，错误情况更明确
			try {
				File dir = new File(parentDirectory);
				boolean isExists = dir.exists();
				if (!isExists) {
					String message = String.format("您设定的目录不存在 : [%s]",
							parentDirectory);
					LOG.error(message);
					throw DataXException.asDataXException(
							ParquetFileReaderErrorCode.FILE_NOT_EXISTS, message);
				}
			} catch (SecurityException se) {
				String message = String.format("您没有权限查看目录 : [%s]",
						parentDirectory);
				LOG.error(message);
				throw DataXException.asDataXException(
						ParquetFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
			}

			directoryRover(regexPath, parentDirectory, toBeReadFiles);
		}

		private void directoryRover(String regexPath, String parentDirectory,
				Set<String> toBeReadFiles) {
			File directory = new File(parentDirectory);
			// is a normal file
			if (!directory.isDirectory()) {
				if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
					toBeReadFiles.add(parentDirectory);
					LOG.info(String.format(
							"add file [%s] as a candidate to be read.",
							parentDirectory));

				}
			} else {
				// 是目录
				try {
					// warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
					File[] files = directory.listFiles();
					if (null != files) {
						for (File subFileNames : files) {
							directoryRover(regexPath,
									subFileNames.getAbsolutePath(),
									toBeReadFiles);
						}
					} else {
						// warn: 对于没有权限的文件，是直接throw DataXException
						String message = String.format("您没有权限查看目录 : [%s]",
								directory);
						LOG.error(message);
						throw DataXException.asDataXException(
								ParquetFileReaderErrorCode.SECURITY_NOT_ENOUGH,
								message);
					}

				} catch (SecurityException e) {
					String message = String.format("您没有权限查看目录 : [%s]",
							directory);
					LOG.error(message);
					throw DataXException.asDataXException(
							ParquetFileReaderErrorCode.SECURITY_NOT_ENOUGH,
							message, e);
				}
			}
		}

		// 正则过滤
		private boolean isTargetFile(String regexPath, String absoluteFilePath) {
			if (this.isRegexPath.get(regexPath)) {
				return this.pattern.get(regexPath).matcher(absoluteFilePath)
						.matches();
			} else {
				return true;
			}

		}

		private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
				int adviceNumber) {
			List<List<T>> splitedList = new ArrayList<List<T>>();
			int averageLength = sourceList.size() / adviceNumber;
			averageLength = averageLength == 0 ? 1 : averageLength;

			for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
				end = begin + averageLength;
				if (end > sourceList.size()) {
					end = sourceList.size();
				}
				splitedList.add(sourceList.subList(begin, end));
			}
			return splitedList;
		}

	}

	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration readerSliceConfig;
		private List<String> sourceFiles;

		@Override
		public void init() {
			this.readerSliceConfig = this.getPluginJobConf();
			this.sourceFiles = this.readerSliceConfig.getList(
					Constant.SOURCE_FILES, String.class);
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
		public void startRead(RecordSender recordSender) {
			LOG.debug("start read source files...");
			for (String fileName : this.sourceFiles) {
				LOG.info(String.format("reading file : [%s]", fileName));
				GroupReadSupport readSupport = new GroupReadSupport();
				Builder<Group> reader;
				try {
					reader = ParquetReader.builder(readSupport, new Path(fileName));
					ParquetReaderUtil.readFromParquet(reader, fileName, readerSliceConfig, recordSender, this.getTaskPluginCollector());
					recordSender.flush();
				} catch (Exception e) {
					// warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
					String message = String
							.format("读取文件异常 : [%s]", e.getLocalizedMessage());
					LOG.error(message);
					throw DataXException.asDataXException(
							ParquetFileReaderErrorCode.RUNTIME_EXCEPTION, message);
				}
			}
			LOG.debug("end read source files...");
		}

	}
}
