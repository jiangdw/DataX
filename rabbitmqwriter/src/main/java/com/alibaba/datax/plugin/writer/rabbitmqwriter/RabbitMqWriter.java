package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by jiangdw on 2020-09-09.
 */
public class RabbitMqWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = this.getPluginJobConf();
            String queue = this.originalConfig.getString(Key.QUEUE, null);
            this.validateParameter();
            String dateFormatOld = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FORMAT);
            String dateFormatNew = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT);
            if (null == dateFormatNew) {
                this.writerSliceConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT,
                                dateFormatOld);
            }
            if (null != dateFormatOld) {
                LOG.warn("您使用format配置日期格式化, 这是不推荐的行为, 请优先使用dateFormat配置项, 两项同时存在则使用dateFormat.");
            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            TxtFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    TxtFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
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
            for (int i = 0; i < adviceNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private AMQP.BasicPropertis props;

        private String queue;
        private String routingKey;
        private String exchange;

        private Connection connection = null;
        private Channel channel = null;
        private String result = null;

        private String host;
        private Integer port;
        private String username;
        private String password;
        private String vhost;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            queue = this.writerSliceConfig.getString(Key.QUEUE, null);
            routingKey = this.writerSliceConfig.getString(Key.ROUTINGKEY, null);
            exchange = this.writerSliceConfig.getString(Key.EXCHANGE, null);
            host = this.writerSliceConfig.getString(Key.HOST, "127.0.0.1");
            port = this.writerSliceConfig.getInt(Key.PORT, 5672);
            username = this.writerSliceConfig.getString(Key.USERNAME, "admin");
            password = this.writerSliceConfig.getString(Key.PASSWORD, "");
            vhost = this.writerSliceConfig.getString(Key.VHOST, "/");

            LOG.info("queue: " + queue + ", routingKey: " + routingKey + ", exchange: " + exchange + ", host: " + host + ", port: " + port + ", username: " + username + ", password: " + password);
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
                Channel channel = connection.createChanel();
                channel.queueDeclare(queue, true, false, false, null);
            } catch (Exception e) {
                LOG.error("Rabbit mq 建立连接失败：" + e.getMessage());
            }
        }

        public int getPort() {
            return null == port ? 5672 : port;
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            OutputStream outputStream = null;
            try {
                File newFile = new File(fileFullPath);
                newFile.createNewFile();
                outputStream = new FileOutputStream(newFile);
                UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
                        outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            } catch (IOException ioe) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.Write_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), ioe);
            } finally {
                IOUtils.closeQuietly(outputStream);
            }
            LOG.info("end do write");
        }

        private String buildFilePath() {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX:
                isEndWithSeparator = this.path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR));
                break;
            case IOUtils.DIR_SEPARATOR_WINDOWS:
                isEndWithSeparator = this.path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                break;
            default:
                break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return String.format("%s%s", this.path, this.fileName);
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
