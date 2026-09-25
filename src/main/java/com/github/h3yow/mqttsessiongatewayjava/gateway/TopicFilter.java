package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 主题过滤器：负责合法性校验与主题匹配。
 * 规则：'+' 匹配单层且必须独占一层；'#' 匹配其自身及之后所有层，
 * 必须独占一层且只能出现在最后一层。空层级（如 "a//b"、首尾 '/'）非法。
 */
public final class TopicFilter {

    private final String raw;
    private final String[] levels;

    private TopicFilter(String raw, String[] levels) {
        this.raw = raw;
        this.levels = levels;
    }

    /**
     * 解析并校验过滤器，非法时抛出携带可区分原因的 {@link GatewayException}。
     */
    public static TopicFilter parse(String raw) {
        if (raw == null || raw.isEmpty()) {
            throw new GatewayException(RejectReason.FILTER_EMPTY_LEVEL, "filter is empty");
        }
        String[] levels = raw.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String lv = levels[i];
            if (lv.isEmpty()) {
                throw new GatewayException(RejectReason.FILTER_EMPTY_LEVEL,
                        "empty level at index " + i + " in filter '" + raw + "'");
            }
            boolean hasHash = lv.indexOf('#') >= 0;
            boolean hasPlus = lv.indexOf('+') >= 0;
            if (hasHash || hasPlus) {
                // 通配符必须独占一层
                if (lv.length() > 1) {
                    throw new GatewayException(RejectReason.FILTER_WILDCARD_MISPLACED,
                            "wildcard must occupy a whole level: '" + lv + "' in filter '" + raw + "'");
                }
                // '#' 只能出现在最后一层
                if (hasHash && i != levels.length - 1) {
                    throw new GatewayException(RejectReason.FILTER_WILDCARD_MISPLACED,
                            "'#' must be the last level in filter '" + raw + "'");
                }
            }
        }
        return new TopicFilter(raw, levels);
    }

    /**
     * 校验发布主题合法性：非空、无空层级、不含通配符。
     */
    public static void validateTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new GatewayException(RejectReason.TOPIC_INVALID, "topic is empty");
        }
        if (topic.indexOf('#') >= 0 || topic.indexOf('+') >= 0) {
            throw new GatewayException(RejectReason.TOPIC_INVALID,
                    "publish topic must not contain wildcards: '" + topic + "'");
        }
        String[] levels = topic.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            if (levels[i].isEmpty()) {
                throw new GatewayException(RejectReason.TOPIC_INVALID,
                        "empty level at index " + i + " in topic '" + topic + "'");
            }
        }
    }

    /**
     * 判断给定发布主题是否匹配本过滤器。调用前主题应已通过 {@link #validateTopic}。
     */
    public boolean matches(String topic) {
        String[] topicLevels = topic.split("/", -1);
        int ti = 0;
        for (String lv : levels) {
            if (lv.equals("#")) {
                return true; // '#' 匹配剩余所有层（含零层）
            }
            if (ti >= topicLevels.length) {
                return false; // 主题层级不足
            }
            if (!lv.equals("+") && !lv.equals(topicLevels[ti])) {
                return false;
            }
            ti++;
        }
        return ti == topicLevels.length; // 过滤器用尽时主题也必须用尽
    }

    public String raw() {
        return raw;
    }
}
