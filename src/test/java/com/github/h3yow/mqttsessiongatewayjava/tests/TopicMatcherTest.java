package com.github.h3yow.mqttsessiongatewayjava.tests;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;
import com.github.h3yow.mqttsessiongatewayjava.gateway.topic.InvalidTopicException;
import com.github.h3yow.mqttsessiongatewayjava.gateway.topic.TopicMatcher;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 通配符边界：单层 '+'、多层 '#'、空层级、非法位置与非法字符。
 */
class TopicMatcherTest {

    private static final Logger log = LoggerFactory.getLogger(TopicMatcherTest.class);

    private void assertMatch(String filter, String topic, boolean expected) {
        boolean actual = TopicMatcher.matches(TopicMatcher.validateFilter(filter),
                TopicMatcher.validateTopic(topic));
        log.info("匹配判定: filter='{}' topic='{}' => {} (期望 {})",
                filter, topic, actual, expected);
        assertEquals(expected, actual,
                () -> "filter=" + filter + " topic=" + topic);
    }

    @Test
    void exactAndSingleLevelWildcard() {
        assertMatch("a/b/c", "a/b/c", true);
        assertMatch("a/b/c", "a/b/d", false);
        assertMatch("a/+/c", "a/b/c", true);
        assertMatch("a/+/c", "a/x/c", true);
        assertMatch("a/+/c", "a/b/x", false);
        assertMatch("a/+", "a/b", true);
        assertMatch("a/+", "a/b/c", false);
        assertMatch("+", "a", true);
        assertMatch("+", "a/b", false);
        assertMatch("+/b", "a/b", true);
        // '+' 匹配恰好一个层级，空层级也算一层
        assertMatch("a/+/c", "a//c", true);
    }

    @Test
    void multiLevelWildcard() {
        assertMatch("#", "a", true);
        assertMatch("#", "a/b/c", true);
        assertMatch("a/#", "a", true);          // '#' 匹配零个层级
        assertMatch("a/#", "a/b", true);
        assertMatch("a/#", "a/b/c/d", true);
        assertMatch("a/#", "b/c", false);
        assertMatch("a/+/c/#", "a/x/c/d/e", true);
        assertMatch("a/+/c/#", "a/x/c", true);
        assertMatch("a/+/c/#", "a/x/d", false);
        // 结尾斜杠产生空层级
        assertMatch("a/#", "a/", true);
        assertMatch("a/#", "a//b", true);
        assertMatch("a/", "a/", true);
        assertMatch("a/", "a", false);
        assertMatch("/a", "/a", true);
        assertMatch("/+", "/a", true);
    }

    @Test
    void invalidFiltersGiveDistinctReasons() {
        assertRejectedFilter("a#/b", RejectReason.HASH_NOT_ALONE_IN_LEVEL);
        assertRejectedFilter("a/b#", RejectReason.HASH_NOT_ALONE_IN_LEVEL);
        assertRejectedFilter("#/b", RejectReason.HASH_NOT_AT_END);
        assertRejectedFilter("a/#/c", RejectReason.HASH_NOT_AT_END);
        assertRejectedFilter("a+ /b", RejectReason.ILLEGAL_LEVEL_CHARACTER);
        assertRejectedFilter("a/+b", RejectReason.ILLEGAL_LEVEL_CHARACTER);
        assertRejectedFilter("", RejectReason.EMPTY_FILTER);
    }

    @Test
    void invalidPublishTopics() {
        InvalidTopicException e1 = assertThrows(InvalidTopicException.class,
                () -> TopicMatcher.validateTopic(""));
        assertEquals(RejectReason.EMPTY_TOPIC, e1.reason());
        InvalidTopicException e2 = assertThrows(InvalidTopicException.class,
                () -> TopicMatcher.validateTopic("a/+/c"));
        assertEquals(RejectReason.ILLEGAL_LEVEL_CHARACTER, e2.reason());
        InvalidTopicException e3 = assertThrows(InvalidTopicException.class,
                () -> TopicMatcher.validateTopic("a/#"));
        assertEquals(RejectReason.ILLEGAL_LEVEL_CHARACTER, e3.reason());
        log.info("非法发布主题原因可区分: EMPTY_TOPIC / ILLEGAL_LEVEL_CHARACTER");
    }

    private void assertRejectedFilter(String filter, RejectReason expected) {
        InvalidTopicException e = assertThrows(InvalidTopicException.class,
                () -> TopicMatcher.validateFilter(filter),
                () -> "过滤器应当非法: " + filter);
        log.info("非法过滤器拒绝: filter='{}' => reason={} msg='{}'",
                filter, e.reason(), e.getMessage());
        assertEquals(expected, e.reason());
    }
}
