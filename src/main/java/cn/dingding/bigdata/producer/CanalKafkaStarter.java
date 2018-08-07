package cn.dingding.bigdata.producer;

import cn.dingding.bigdata.AbstractCanalClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.util.concurrent.ExecutorService;

/**
 * kafka 启动类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaStarter {

    private static final String       CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(CanalKafkaStarter.class);

    private volatile static boolean   running              = false;

    private static ExecutorService    executorService;

    private static CanalKafkaProducer canalKafkaProducer;

    private static KafkaProperties    kafkaProperties;

    public static void init() {
        try {
            logger.info("## load kafka configurations");
            String conf = System.getProperty("kafka.conf", "classpath:kafka.yml");

            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                kafkaProperties = new Yaml().loadAs(CanalKafkaStarter.class.getClassLoader().getResourceAsStream(conf),
                    KafkaProperties.class);
            } else {
                kafkaProperties = new Yaml().loadAs(new FileInputStream(conf), KafkaProperties.class);
            }

            // 初始化 kafka producer
            canalKafkaProducer = new CanalKafkaProducer();
            canalKafkaProducer.init(kafkaProperties);

            AbstractCanalClient.setCanalKafkaProducer(canalKafkaProducer);
            // 对应每个instance启动一个worker线程
            running = true;
            logger.info("## the kafka workers is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the kafka workers");
                        running = false;
                        executorService.shutdown();
                        canalKafkaProducer.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping kafka workers:", e);
                    } finally {
                        logger.info("## canal kafka is down.");
                    }
                }

            });

        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal kafka workers:", e);
            System.exit(0);
        }
    }
}
