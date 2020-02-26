package com.cn.gp.es.admin;


import com.cn.gp.common.file.FileCommon;
import com.cn.gp.es.client.ESClientUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> ES创建 </p>
 * @date 2020/2/26
 */
public class AdminUtil {
    private static Logger LOG = LoggerFactory.getLogger(AdminUtil.class);

    /**
     * @param index
     * @param type
     * @param path
     * @param shard
     * @param replication
     * @return
     * @throws Exception
     */
    public static boolean buildIndexAndTypes(String index, String type, String path, int shard, int replication) throws Exception {
        boolean flag;
        TransportClient client = ESClientUtils.getClient();
        String mappingJson = FileCommon.getAbstractPath(path);

        boolean indices = AdminUtil.createIndices(client, index, shard, replication);
        if (indices) {
            LOG.info("创建索引" + index + "成功");
            flag = MappingUtil.addMapping(client, index, type, mappingJson);
        } else {
            LOG.error("创建索引" + index + "失败");
            flag = false;
        }
        return flag;
    }


    /**
     * @return boolean
     * @author GuYongtao
     * <p>判断需要创建的index是否存在</p>
     * @date 2020/2/26
     */
    public static boolean indexExists(TransportClient client, String index) {
        boolean ifExists = false;
        try {
            System.out.println("client===" + client);
            IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(index).execute().actionGet();
            ifExists = existsResponse.isExists();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("判断index是否存在失败...");
            return ifExists;
        }
        return ifExists;
    }


    /**
     * @return boolean
     * @author GuYongtao
     * <p>创建索引</p>
     * @date 2020/2/26
     */
    public static boolean createIndices(TransportClient client, String index, int shard, int replication) {

        if (!indexExists(client, index)) {
            LOG.info("该index不存在，创建...");
            CreateIndexResponse createIndexResponse = null;
            try {
                createIndexResponse = client.admin().indices().prepareCreate(index)
                        .setSettings(Settings.builder()
                                .put("index.number_of_shards", shard)
                                .put("index.number_of_replicas", replication)
                                .put("index.codec", "best_compression")
                                .put("refresh_interval", "30s"))
                        .execute().actionGet();
                return createIndexResponse.isAcknowledged();
            } catch (Exception e) {
                LOG.error(null, e);
                return false;
            }
        }
        LOG.warn("该index: [" + index + "] 已经存在...");
        return false;
    }


}
