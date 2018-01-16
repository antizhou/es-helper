package com.iforfee.es.helper.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author joyo
 * @date 2018/1/16
 */
public class EsConfig {
    public static final TransportClient getClint(String hosts, String clusterName) {
        String[] hostsArr = hosts.split(",");
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("transport.type", "netty3")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            for (String host : hostsArr) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
            }

        } catch (UnknownHostException e) {
        }
        return client;
    }

}
