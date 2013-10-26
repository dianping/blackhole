package com.dp.blackhole.consumer;

public class Partition {
    public final int brokerId;

    public final int partId;

    public Partition(int brokerId, int partId) {
        this.brokerId = brokerId;
        this.partId = partId;
        this.name = brokerId + "-" + partId;
    }

    public Partition(String name) {
        this(1, 1);
    }

    private final String name;

    /**
     * brokerid with partitionid
     * 
     * @return brokerid-partitionid
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    public int compareTo(Partition o) {
        if (this.brokerId == o.brokerId) {
            return this.partId - o.partId;
        }
        return this.brokerId - o.brokerId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + brokerId;
        result = prime * result + partId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Partition other = (Partition) obj;
        if (brokerId != other.brokerId) return false;
        if (partId != other.partId) return false;
        return true;
    }

    public static Partition parse(String s) {
        String[] pieces = s.split("-");
        if (pieces.length != 2) {
            throw new IllegalArgumentException("Expected name in the form x-y.");
        }
        return new Partition(Integer.parseInt(pieces[0]), Integer.parseInt(pieces[1]));
    }
}
