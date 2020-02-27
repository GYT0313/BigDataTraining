package com.cn.gp.es.client;


import com.cn.gp.common.config.ConfigUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Properties;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> ES 客户端获取 </p>
 * @date 2020/2/26
 */
public class ESClientUtils implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(ESClientUtils.class);
    private volatile static TransportClient esClusterClient;

    private ESClientUtils() {
    }

    private static Properties properties;

    static {
        properties = ConfigUtil.getInstance().getProperties("es/es-cluster.properties");
    }

    public static TransportClient getClient() {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        String clusterName = properties.getProperty("es.cluster.name");
        String clusterNodes1 = properties.getProperty("es.cluster.nodes1");
        String tcpPort = properties.getProperty("es.cluster.tcp.port");

        if (esClusterClient == null) {
            synchronized (ESClientUtils.class) {
                if (esClusterClient == null) {
                    try {
                        //关闭自动嗅探功能 - client.transport.sniff
                        Settings settings = Settings.builder()
                                .put("cluster.name", clusterName)
                                .put("client.transport.sniff", false).build();
                        esClusterClient = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(clusterNodes1),
                                        Integer.valueOf(tcpPort)));
                        LOG.info("esClusterClient========" + esClusterClient.listedNodes());
                    } catch (Exception e) {
                        LOG.error("获取客户端失败", e);
                    } finally {

                    }
                }
            }
        }
        return esClusterClient;
    }


    public static void main(String[] args) {
        TransportClient client = ESClientUtils.getClient();
        System.out.println(client);
    }
}
