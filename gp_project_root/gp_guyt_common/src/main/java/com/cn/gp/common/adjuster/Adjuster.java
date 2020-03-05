package com.cn.gp.common.adjuster;

/**
 * @author GuYongtao
 * @version 1.0.0
 * <p> 数据调整接口 </p>
 * @date 2020/1/13
 */
public interface Adjuster<T, E> {
    E doAdjust(T data);
}
