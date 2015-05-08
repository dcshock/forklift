package forklift.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileScannerTest {
    @Test
    public void scan()
      throws IOException, InterruptedException {
        final File tmpDir = Files.createTempDir();
        tmpDir.deleteOnExit();

        final FileScanner fileScan = new FileScanner(tmpDir);
        assertEquals(0, fileScan.scan().size());

        final File file1 = File.createTempFile("test", "test", tmpDir);

        List<FileScanResult> results = fileScan.scan();

        boolean add = false;
        assertEquals(1, results.size());
        for (FileScanResult result : results) {
            if (result.equals(new FileScanResult(FileStatus.Added, file1.getName())))
                add = true;
        }
        assertTrue("File was not added", add);

        boolean unchanged = false;
        results = fileScan.scan();
        assertEquals(1, results.size());
        for (FileScanResult result : results) {
            if (result.equals(new FileScanResult(FileStatus.Unchanged, file1.getName())))
                unchanged = true;
        }
        assertTrue("File was not unchanged", unchanged);

        assertTrue(file1.setLastModified(System.currentTimeMillis() + 10000));
        results = fileScan.scan();
        boolean modified = false;
        assertEquals(1, results.size());
        for (FileScanResult result : results) {
            if (result.equals(new FileScanResult(FileStatus.Modified, file1.getName())))
                modified = true;
        }
        assertTrue("File was not modified", modified);

        final File file2 = File.createTempFile("test", "test", tmpDir);

        // Reset the mod time so that the file scanner reports the file as unchanged.
        file1.setLastModified(fileScan.getLastScanTime());
        results = fileScan.scan();
        boolean newFile = false;
        boolean unchangedFile = false;
        assertEquals(2, results.size());
        for (FileScanResult result : results) {
            if (result.equals(new FileScanResult(FileStatus.Added, file2.getName())))
                newFile = true;
            else if (result.equals(new FileScanResult(FileStatus.Unchanged, file1.getName())))
                unchangedFile = true;
        }
        assertTrue("File 2 was not detected", newFile);
        assertTrue("File 1 was not reported", unchangedFile);

        file1.delete();
        results = fileScan.scan();
        boolean removed = false;
        assertEquals(2, results.size());
        for (FileScanResult result : results) {
            if (result.equals(new FileScanResult(FileStatus.Removed, file1.getName())))
                removed = true;
        }
        assertTrue("File was not removed", removed);
    }
}
