package com.github.h3yow.mqttsessiongatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 主题过滤器：通配符边界与非法过滤器的可区分拒绝原因。
 */
class TopicFilterTest {

    private static final Logger log = LoggerFactory.getLogger(TopicFilterTest.class);

    private void assertMatch(String filter, String topic, boolean expected) {
        boolean actual = TopicFilter.parse(filter).matches(topic);
        log.info("[match] filter='{}' topic='{}' -> expected={} actual={}", filter, topic, expected, actual);
        assertEquals(expected, actual);
    }

    private void assertReject(String filter, RejectReason reason) {
        GatewayException ex = assertThrows(GatewayException.class, () -> TopicFilter.parse(filter));
        log.info("[reject] filter='{}' -> reason={} detail='{}'", filter, ex.reason(), ex.getMessage());
        assertEquals(reason, ex.reason());
    }

    @Test
    void exactMatch() {
        assertMatch("a/b/c", "a/b/c", true);
        assertMatch("a/b/c", "a/b", false);
        assertMatch("a/b/c", "a/b/c/d", false);
        assertMatch("a/b/c", "a/b/x", false);
    }

    @Test
    void singleLevelWildcard() {
        assertMatch("a/+/c", "a/b/c", true);
        assertMatch("a/+/c", "a/x/c", true);
        assertMatch("a/+/c", "a/c", false);        // '+' 必须占满一层，不能跳过
        assertMatch("a/+/c", "a/b/c/d", false);    // '+' 只匹配单层
        assertMatch("a/+", "a/b", true);
        assertMatch("+", "a", true);
        assertMatch("+", "a/b", false);
        assertMatch("+/+/+", "a/b/c", true);
        assertMatch("+/+/+", "a/b", false);
    }

    @Test
    void multiLevelWildcard() {
        assertMatch("a/#", "a", true);             // '#' 可匹配零层
        assertMatch("a/#", "a/b", true);
        assertMatch("a/#", "a/b/c/d", true);
        assertMatch("#", "a", true);
        assertMatch("#", "a/b/c", true);
        assertMatch("a/#", "b/c", false);
        assertMatch("a/b/#", "a/b", true);
        assertMatch("a/b/#", "a/c", false);
    }

    @Test
    void mixedWildcards() {
        assertMatch("a/+/#", "a/b", true);
        assertMatch("a/+/#", "a/b/c/d", true);
        assertMatch("a/+/#", "a", false);          // '+' 层必须存在
        assertMatch("+/+/#", "x/y/z", true);
    }

    @Test
    void invalidFiltersWithDistinguishableReasons() {
        assertReject("", RejectReason.FILTER_EMPTY_LEVEL);
        assertReject("a//b", RejectReason.FILTER_EMPTY_LEVEL);
        assertReject("/a", RejectReason.FILTER_EMPTY_LEVEL);
        assertReject("a/", RejectReason.FILTER_EMPTY_LEVEL);
        assertReject("a#/b", RejectReason.FILTER_WILDCARD_MISPLACED);   // '#' 未独占一层
        assertReject("a/b+", RejectReason.FILTER_WILDCARD_MISPLACED);   // '+' 未独占一层
        assertReject("a/#/b", RejectReason.FILTER_WILDCARD_MISPLACED);  // '#' 不在最后一层
        assertReject("a/+b/c", RejectReason.FILTER_WILDCARD_MISPLACED);
    }

    @Test
    void invalidPublishTopics() {
        for (String topic : new String[]{"", "a//b", "a/+/c", "a/#", "/a"}) {
            GatewayException ex = assertThrows(GatewayException.class,
                    () -> TopicFilter.validateTopic(topic));
            log.info("[topic-reject] topic='{}' -> reason={}", topic, ex.reason());
            assertEquals(RejectReason.TOPIC_INVALID, ex.reason());
        }
        TopicFilter.validateTopic("a/b/c"); // 合法主题不抛异常
    }
}
