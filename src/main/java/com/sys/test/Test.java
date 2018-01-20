package com.sys.test;

import com.sys.util.ProducerUtil;

/**
 * created by My System on 19-Jan-18
 **/
public class Test {
    public static void main(String[] args) {
        ProducerUtil.sendMessage("mytopic","hello");
    }
}
