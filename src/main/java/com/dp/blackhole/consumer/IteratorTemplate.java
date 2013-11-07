package com.dp.blackhole.consumer;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class IteratorTemplate<T> implements Iterator<T> {


    enum State {
        DONE, READY, NOT_READY, FAILED;
    }

    private State state = State.NOT_READY;

    private T nextItem = null;

    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.NOT_READY;
        return nextItem;
    }

    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                return false;
            case READY:
                return true;
            case NOT_READY:
                break;
        }
        return maybeComputeNext();
    }

    protected abstract T makeNext();

    private boolean maybeComputeNext() {
        state = State.FAILED;
        nextItem = makeNext();
        if (state == State.DONE) return false;
        state = State.READY;
        return true;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected void resetState() {
        state = State.NOT_READY;
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }

}
