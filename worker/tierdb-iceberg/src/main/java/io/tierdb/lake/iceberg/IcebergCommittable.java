package io.tierdb.lake.iceberg;

import java.util.List;
import org.apache.iceberg.DataFile;

/** All data files of one tiering op, to be committed as ONE Iceberg snapshot. */
public record IcebergCommittable(List<DataFile> dataFiles) {
    public IcebergCommittable {
        dataFiles = List.copyOf(dataFiles);
    }
}
