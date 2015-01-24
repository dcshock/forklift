package forklift.file;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FileScanner {
    private long lastScan = Long.MIN_VALUE;
    private Map<String, Object> files = new HashMap<>();
    private Object placeHolder = new Object();

    private File dir;

    public FileScanner(File dir) {
        this.dir = dir;
    }

    public List<FileScanResult> scan() {
        final List<FileScanResult> results = new ArrayList<>();

        // Scan for added files.
        for (String filename : dir.list()) {
            final File file = new File(dir, filename);
            if (file.isDirectory())
                continue;

            boolean changed = file.lastModified() > lastScan;
            if (files.containsKey(filename)) {
                if (changed)
                    results.add(new FileScanResult(FileStatus.Modified, filename));
                else
                    results.add(new FileScanResult(FileStatus.Unchanged, filename));
            } else {
                results.add(new FileScanResult(FileStatus.Added, filename));
                files.put(filename, placeHolder);
            }
        }

        // Scan for removed files.
        final Iterator<String> it = files.keySet().iterator();
        while (it.hasNext()) {
            final String filename = it.next();
            if (!results.contains(new FileScanResult(FileStatus.Unchanged, filename)) &&
                !results.contains(new FileScanResult(FileStatus.Added, filename)) &&
                !results.contains(new FileScanResult(FileStatus.Modified, filename))) {
                it.remove();
                results.add(new FileScanResult(FileStatus.Removed, filename));
            }
        }

        lastScan = System.currentTimeMillis();
        return results;
    }

    public File getDir() {
        return dir;
    }

    public long getLastScanTime() {
        return lastScan;
    }
}
