package com.cn.gp.common.thread;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 线程池管理器单例
 * 默认创建   ewCachedThreadPool ：创建一个可缓存的线程池
 * 可通过指定线程的数量来创建：newFixedThreadPool  ： 创建固定大小的线程池
 * </p>
 * @date 2020/1/14
 */
public class ThreadPoolManager implements Serializable {
    private static final long serialVersionUID = 4409788662655238097L;
    private static ThreadPoolManager tpm;
    private transient ExecutorService newCachedThreadPool;
    private transient ExecutorService newFixedThreadPoo;

    private int poolCapacity;

    private ThreadPoolManager() {
        if (newCachedThreadPool == null) {
            newCachedThreadPool = Executors.newCachedThreadPool();
        }
    }

    public static ThreadPoolManager getInstance() {
        if (tpm == null) {
            synchronized (ThreadPoolManager.class) {
                if (tpm == null) {
                    tpm = new ThreadPoolManager();
                }
            }
        }
        return tpm;
    }

    public ExecutorService getExecutorService() {
        if (newCachedThreadPool == null) {
            synchronized (ThreadPoolManager.class) {
                if (newCachedThreadPool == null) {
                    newCachedThreadPool = Executors.newCachedThreadPool();
                }
            }
        }
        return newCachedThreadPool;
    }

    public ExecutorService getExecutorService(int poolCapacity) {
        return getExecutorService(poolCapacity, false);
    }

    public synchronized ExecutorService getExecutorService(int poolCapacity, boolean closeOld) {
        if (newFixedThreadPoo == null || (this.poolCapacity != poolCapacity)) {
            if (newFixedThreadPoo != null && closeOld) {
                newFixedThreadPoo.shutdown();
            }
            newFixedThreadPoo = Executors.newFixedThreadPool(poolCapacity);
            this.poolCapacity = poolCapacity;
        }
        return newFixedThreadPoo;
    }

}
