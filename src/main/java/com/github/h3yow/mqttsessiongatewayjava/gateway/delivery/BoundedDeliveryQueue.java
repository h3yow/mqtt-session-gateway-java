package com.github.h3yow.mqttsessiongatewayjava.gateway.delivery;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 支持“队首插入”的有界阻塞队列，用于客户端投递管道。
 *
 * <ul>
 *   <li>{@link #offer(Object, long, TimeUnit)}：发布线程在队尾入队，满了按超时背压；</li>
 *   <li>{@link #putFront(Object)}：worker 投递失败/断连回灌时把消息放回队首优先重试；
 *       该消息此前占用的槽位刚被释放，正常情况下立即可插入。</li>
 * </ul>
 */
public class BoundedDeliveryQueue<E> {

    private final int capacity;
    private final Deque<E> deque;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public BoundedDeliveryQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.deque = new ArrayDeque<>(capacity);
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (deque.size() >= capacity) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            deque.addLast(e);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 队首插入；队列满时阻塞等待空位（仅用于“刚取出又放回”的回灌路径）。
     */
    public void putFront(E e) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (deque.size() >= capacity) {
                notFull.await();
            }
            deque.addFirst(e);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty()) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            E e = deque.pollFirst();
            notFull.signal();
            return e;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return deque.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 非阻塞队尾入队（容量保证不小于持久化上限，仅用于启动恢复）。
     *
     * @throws IllegalStateException 队列已满
     */
    public void add(E e) {
        lock.lock();
        try {
            if (deque.size() >= capacity) {
                throw new IllegalStateException("队列已满，无法 add");
            }
            deque.addLast(e);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public int drainTo(Collection<? super E> sink) {
        lock.lock();
        try {
            int n = 0;
            E e;
            while ((e = deque.pollFirst()) != null) {
                sink.add(e);
                n++;
            }
            if (n > 0) {
                notFull.signalAll();
            }
            return n;
        } finally {
            lock.unlock();
        }
    }
}
