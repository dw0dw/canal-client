package cn.dingding.bigdata;

import cn.dingding.bigdata.producer.CanalKafkaProducer;
import cn.dingding.bigdata.util.ColumnJson;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class AbstractCanalClient {

    protected final static Logger             logger             = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String             SEP                = SystemUtils.LINE_SEPARATOR;
    protected static final String             DATE_FORMAT        = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean                running            = false;
    protected Thread.UncaughtExceptionHandler handler            = new Thread.UncaughtExceptionHandler() {

                                                                     public void uncaughtException(Thread t, Throwable e) {
                                                                         logger.error("parse events has an error and reconnect in 5000 ms ", e);
                                                                         try {
                                                                             //异常 重新连接
                                                                             Thread.sleep(5000);
                                                                             thread=null;
                                                                             start();
                                                                         } catch (InterruptedException e1) {
                                                                             e1.printStackTrace();
                                                                         }


                                                                     }
                                                                 };
    protected Thread                          thread             = null;
    protected CanalConnector connector;
    protected static String                   context_format     = null;
    protected static String                   row_format         = null;
    protected static String                   transaction_format = null;
    protected String                          destination;
    private static CanalKafkaProducer canalKafkaProducer;
    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                     + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                     + SEP;

        transaction_format = SEP
                             + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                             + SEP;

    }

    public AbstractCanalClient(String destination){
        this(destination, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector){
        this.destination = destination;
        this.connector = connector;
    }

    public static CanalKafkaProducer getCanalKafkaProducer() {
        return canalKafkaProducer;
    }

    public static void setCanalKafkaProducer(CanalKafkaProducer canalKafkaProducer) {
        AbstractCanalClient.canalKafkaProducer = canalKafkaProducer;
    }

    protected void start() {

        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        connector.stopRunning();
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
        BigDecimal id = new BigDecimal(0);
        int batchSize = 5 * 1024;
        long batchId = 0L;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {

                    } else {
//                        printSummary(message, batchId, size);
                        JSONObject js = new JSONObject();
                        long executeTime =new Date().getTime();

                        printEntry(message.getEntries(),js,canalKafkaProducer);
                        long delayTime = new Date().getTime() - executeTime;
                        System.out.println("spend time:-"+delayTime);
//                        if(id != null){
//                            BigDecimal  part = id.divideAndRemainder(BigDecimal.valueOf(11))[1];
//                            topic.setPartition(part.intValue());
//                        }
//                        canalKafkaProducer.send(topic,message,js);
                    }
                    connector.ack(batchId); // 提交确认
//                    connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                logger.error("process error!", e);
                connector.rollback(batchId);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[] { batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition });
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        String position = entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                          + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
        if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
            position += " gtid(" + entry.getHeader().getGtid() + ")";
        }
        return position;
    }

    protected void printEntry(List<Entry> entrys,JSONObject js,CanalKafkaProducer canalKafkaProducer) {
        BigDecimal id =null;
        int partation =0;
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            Date date = new Date(entry.getHeader().getExecuteTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
//                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
//                    TransactionBegin begin = null;
//                    try {
//                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
//                    } catch (InvalidProtocolBufferException e) {
//                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
//                    }
//                    // 打印事务头信息，执行的线程id，事务耗时
//                    logger.info(transaction_format,
//                        new Object[] { entry.getHeader().getLogfileName(),
//                                String.valueOf(entry.getHeader().getLogfileOffset()),
//                                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
//                                entry.getHeader().getGtid(), String.valueOf(delayTime) });
//
//                    js.put(DataJsonTab.LOG_FILE_NAME,entry.getHeader().getLogfileName());
//                    js.put(DataJsonTab.LOG_FILE_OFFSET,entry.getHeader().getLogfileOffset());
//                    js.put(DataJsonTab.BEGIN_EXECUTE_TIME,entry.getHeader().getExecuteTime());
//                    js.put(DataJsonTab.GT_ID,entry.getHeader().getGtid());
//                    js.put(DataJsonTab.THREAD_ID,begin.getThreadId());
//                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
//                    printXAInfo(begin.getPropsList());
//                } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
//                    TransactionEnd end = null;
//                    try {
//                        end = TransactionEnd.parseFrom(entry.getStoreValue());
//                    } catch (InvalidProtocolBufferException e) {
//                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
//                    }
//                    // 打印事务提交信息，事务id
//                    logger.info("----------------\n");
//                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
//                    printXAInfo(end.getPropsList());
//                    logger.info(transaction_format,
//                        new Object[] { entry.getHeader().getLogfileName(),
//                                String.valueOf(entry.getHeader().getLogfileOffset()),
//                                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
//                                entry.getHeader().getGtid(), String.valueOf(delayTime) });
//
//                    js.put(DataJsonTab.END_EXECUTE_TIME,entry.getHeader().getExecuteTime());
//                    js.put(DataJsonTab.TRANSACTION,end.getTransactionId());
//
//                }

                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                    new Object[] { entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                            entry.getHeader().getTableName(), eventType,
                            String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                            entry.getHeader().getGtid(), String.valueOf(delayTime) });

                partation = entry.getHeader().getSchemaName().hashCode()+entry.getHeader().getTableName().hashCode();

                partation = partation >0?partation:-partation;
//                topic.setPartition(partation % 11);

                js.put(DataJsonTab.DATABASE,entry.getHeader().getSchemaName());
                js.put(DataJsonTab.TABLE_NAME,entry.getHeader().getTableName());

                js.put(DataJsonTab.LOG_FILE_OFFSET,entry.getHeader().getLogfileOffset());
                js.put(DataJsonTab.LOG_FILE_NAME,entry.getHeader().getLogfileName());

                js.put(DataJsonTab.EVENT_TYPE,entry.getHeader().getEventType());
                js.put(DataJsonTab.EXECUTE_TIME,entry.getHeader().getExecuteTime());
                js.put(DataJsonTab.DELAY_TIME,delayTime);
                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    js.put(DataJsonTab.QUERY_SQL,rowChage.getSql());
                    continue;
                }

//                printXAInfo(rowChage.getPropsList());
                JSONArray array = new JSONArray();
//                int count=0;
                for (RowData rowData : rowChage.getRowDatasList()) {
                    JSONObject data = new JSONObject();
                    if (eventType == EventType.DELETE) {
                        array = printColumn(rowData.getBeforeColumnsList(),data);
//                        js.put(DataJsonTab.DATA,data);
//                        array.add(data);
                    } else if (eventType == EventType.INSERT) {
                        array = printColumn(rowData.getAfterColumnsList(),data);
//                        array.add(data);
//                        js.put(DataJsonTab.DATA,data);
                    } else {
                        array = printColumn(rowData.getAfterColumnsList(),data);
//                        array.add(data);
//                        js.put(DataJsonTab.DATA,data);
                    }

                    js.put(DataJsonTab.DATA,data);
                    js.put(DataJsonTab.PKS,array);
                    canalKafkaProducer.send(partation,entry,js);

//                    if(count == 10){
//                        topic.setPartition(partation % 11);
//                        js.put(DataJsonTab.DATA,array);
//                        canalKafkaProducer.send(topic,entry,js);
//                        array.clear();
//                        count=0;
//                    }
//                    count++;
                }
//                topic.setPartition(partation % 11);
//                js.put(DataJsonTab.DATA,array);
//                canalKafkaProducer.send(topic,entry,js);
            }
        }
    }

    protected JSONArray printColumn(List<Column> columns,JSONObject data) {
        JSONArray array = new JSONArray();
        for (Column column : columns) {
            data.put(column.getName(), ColumnJson.convent(column));
            if(column.getIsKey()){
                array.add(column.getName());
            }

//            StringBuilder builder = new StringBuilder();
//            builder.append(column.getName() + " : " + column.getValue());
//            builder.append("    type=" + column.getMysqlType());
//            if (column.getUpdated()) {
//                builder.append("    update=" + column.getUpdated());
//            }
//            builder.append(SEP);
//            logger.info(builder.toString());
        }
        return array;
    }

    protected void printXAInfo(List<Pair> pairs) {
        if (pairs == null) {
            return;
        }

        String xaType = null;
        String xaXid = null;
        for (Pair pair : pairs) {
            String key = pair.getKey();
            if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }

        if (xaType != null && xaXid != null) {
            logger.info(" ------> " + xaType + " " + xaXid);
        }
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

}
