package forklift.file;

public class FileScanResult {
    private FileStatus status;
    private String filename;

    FileScanResult(FileStatus status, String filename) {
        this.status = status;
        this.filename = filename;
    }

    public FileStatus getStatus() {
        return status;
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((filename == null) ? 0 : filename.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FileScanResult other = (FileScanResult) obj;
        if (filename == null) {
            if (other.filename != null)
                return false;
        } else if (!filename.equals(other.filename))
            return false;
        if (status != other.status)
            return false;
        return true;
    }
}
