package com.lpy.elasticsearch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;

/**
 * @author lipengyu
 * @date 2019/6/1 17:13
 */
public class EmployeeCRUDApp {

    @Test
    public void test() throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

//        createEmployee(client);
        getEmployee(client);
//        updateEmployee(client);
//        deleteEmployee(client);
        client.close();
    }

    /**
     * 创建员工信息（创建一个document）
     * @param client
     */
    private void createEmployee(TransportClient client) throws Exception {
        IndexResponse response = client.prepareIndex("employee", "_doc", "1")
                .setSource(XContentFactory.jsonBuilder()
                .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 获取员工信息
     * @param client
     * @throws Exception
     */
    private void getEmployee(TransportClient client) throws Exception {
        GetResponse response = client.prepareGet("employee", "_doc", "1").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 修改员工信息
     * @param client
     * @throws Exception
     */
    private void updateEmployee(TransportClient client) throws Exception {
        UpdateResponse response = client.prepareUpdate("employee", "_doc", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("position", "technique manager")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 删除 员工信息
     * @param client
     * @throws Exception
     */
    private void deleteEmployee(TransportClient client) throws Exception {
        DeleteResponse response = client.prepareDelete("employee", "_doc", "1").get();
        System.out.println(response.getResult());
    }
}









































