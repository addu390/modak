package io.modak.lake.iceberg;

import java.util.List;
import org.apache.iceberg.DataFile;

/** Files one partition's flush produced, not yet visible without a snapshot commit. */
public record IcebergWriteResult(List<DataFile> dataFiles) {
    public IcebergWriteResult {
        dataFiles = List.copyOf(dataFiles);
    }
}
