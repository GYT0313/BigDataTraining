package com.cn.gp.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Iterator;
import java.util.Map;

public class HiveConf {
    private static HiveConf hiveConf;
    private static HiveContext hiveContext;
    private static final String DEFAULT_CONFIG = "spark/hive/hive-site.xml";

    private HiveConf() {
    }

    public static HiveConf getInstance() {
        if (hiveConf == null) {
            synchronized (HiveConf.class) {
                if (hiveConf == null) {
                    hiveConf = new HiveConf();
                }
            }
        }
        return hiveConf;
    }

    public static HiveContext getHiveContext(SparkContext sparkContext) {
        if (hiveContext == null) {
            synchronized (HiveConf.class) {
                if (hiveContext == null) {
                    hiveContext = new HiveContext(sparkContext);
                    Configuration configuration = new Configuration();
                    configuration.addResource(DEFAULT_CONFIG);
                    Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> next = iterator.next();
                        hiveContext.setConf(next.getKey(), next.getValue());
                    }
                    hiveContext.setConf("spark.sql.parquet.mergeSchema", "true");
                }
            }
        }
        return hiveContext;
    }
}
