package com.idstaa.kafka.demo;

/**
 * @author chenjie
 * @date 2021/3/26 14:42
 */
public class test {
    public static void main(String[] args) {
        String str = "console-consumer-88242";
        System.out.println(str.hashCode()%50);
    }
}
