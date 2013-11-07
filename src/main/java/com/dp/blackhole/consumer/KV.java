package com.dp.blackhole.consumer;

public class KV<K, V> {

    public final K k;

    public final V v;

    public KV(K k, V v) {
        super();
        this.k = k;
        this.v = v;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((k == null) ? 0 : k.hashCode());
        result = prime * result + ((v == null) ? 0 : v.hashCode());
        return result;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        KV other = (KV) obj;
        if (k == null) {
            if (other.k != null) return false;
        } else if (!k.equals(other.k)) return false;
        if (v == null) {
            if (other.v != null) return false;
        } else if (!v.equals(other.v)) return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("KV [k=%s, v=%s]", k, v);
    }
}
