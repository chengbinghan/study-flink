package com.hcb.flink.window.flow.wordcount;

/**
 * @author ChengBing Han
 * @date 20:53  2018/11/19
 * @description
 */
public class WordWithCount {
    public String word;
    public Long count;

    public WordWithCount(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    public WordWithCount() {
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}

//算子计算
