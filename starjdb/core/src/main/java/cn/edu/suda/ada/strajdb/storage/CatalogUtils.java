package cn.edu.suda.ada.strajdb.storage;

import lombok.var;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;

import static cn.edu.suda.ada.strajdb.storage.IcebergUtils.getConfig;

public class CatalogUtils {
    final public static ThreadLocal<Catalog> catalogLocal = ThreadLocal.withInitial(() -> {
        var catalog = new JdbcCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("strajdb", getConfig());
        return catalog;
    });
    private CatalogUtils(){}
}
