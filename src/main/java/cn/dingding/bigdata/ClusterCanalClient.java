package cn.dingding.bigdata;

import cn.dingding.bigdata.client.CanalClientProperties;
import cn.dingding.bigdata.producer.CanalKafkaStarter;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class ClusterCanalClient extends AbstractCanalClient {

    public ClusterCanalClient(String destination){
        super(destination);
    }
    private static final String       CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(ClusterCanalClient.class);
    private static CanalClientProperties canalClientProperties;
    public static void main(String args[]) throws FileNotFoundException {

        logger.info("## load canal configurations");
        String conf = System.getProperty("application.conf", "classpath:application.yml");
        try {
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                canalClientProperties = new Yaml().loadAs(CanalKafkaStarter.class.getClassLoader().getResourceAsStream(conf),
                        CanalClientProperties.class);
            } else {
                canalClientProperties = new Yaml().loadAs(new FileInputStream(conf), CanalClientProperties.class);
            }
        }catch (Exception ex){
            throw new FileNotFoundException("application.yml and application.conf no found");

        }

        String destinations = canalClientProperties.getDestination();
        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover

        String[] destinationA = destinations.split(",");
        for (String destination:destinationA){
            startCanalConnector(canalClientProperties.getZkServers(),destination);
        }



    }

    private static void startCanalConnector(String zkServers,String destination){

        CanalConnector connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
        final ClusterCanalClient client = new ClusterCanalClient(destination);
        client.setConnector(connector);
        client.start();
        CanalKafkaStarter.init();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    client.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:", e);
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }
}
