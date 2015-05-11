package forklift;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Registrar<T> {
    protected List<T> records = new ArrayList<T>();

    public synchronized void register(T t) {
        records.add(t);
    }

    public synchronized T unregister(T t) {
        Iterator<T> it = records.iterator();
        while (it.hasNext()) {
            T itT = it.next();
            if (itT.equals(t)) {
                it.remove();
                return itT;
            }
        }
        return null;
    }

    public synchronized boolean isRegistered(T t) {
        Iterator<T> it = records.iterator();
        while (it.hasNext()) {
            T itT = it.next();
            if (itT.equals(t))
                return true;
        }
        return false;
    }

    public synchronized List<T> getAll() {
        return records;
    }
}