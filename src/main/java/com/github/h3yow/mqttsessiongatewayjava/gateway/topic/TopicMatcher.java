package com.github.h3yow.mqttsessiongatewayjava.gateway.topic;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;

/**
 * 主题过滤器解析与匹配。
 *
 * <p>层级规则（与 MQTT 语义一致）：
 * <ul>
 *   <li>主题与过滤器都以 {@code '/'} 分隔层级，允许前导/结尾/连续斜杠（产生空层级，空层级是合法层级）。</li>
 *   <li>{@code '+'} 单层通配符：必须独占一个层级，匹配任意一个层级（含空层级）。</li>
 *   <li>{@code '#'} 多层通配符：必须独占一个层级且只能位于最后一层，匹配其后零或多个层级。</li>
 *   <li>发布主题不允许出现任何通配符；空主题/空过滤器一律拒绝。</li>
 * </ul>
 */
public final class TopicMatcher {

    private TopicMatcher() {
    }

    /**
     * 校验发布主题，返回原主题。
     *
     * @throws InvalidTopicException EMPTY_TOPIC 或 ILLEGAL_LEVEL_CHARACTER
     */
    public static String validateTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new InvalidTopicException(RejectReason.EMPTY_TOPIC, "发布主题不能为空");
        }
        String[] levels = split(topic);
        for (String level : levels) {
            if (level.indexOf('+') >= 0 || level.indexOf('#') >= 0) {
                throw new InvalidTopicException(RejectReason.ILLEGAL_LEVEL_CHARACTER,
                        "发布主题不允许出现通配符: " + topic);
            }
        }
        return topic;
    }

    /**
     * 校验并返回过滤器。
     *
     * @throws InvalidTopicException EMPTY_FILTER / HASH_NOT_AT_END /
     *                               HASH_NOT_ALONE_IN_LEVEL / ILLEGAL_LEVEL_CHARACTER
     */
    public static String validateFilter(String filter) {
        if (filter == null || filter.isEmpty()) {
            throw new InvalidTopicException(RejectReason.EMPTY_FILTER, "主题过滤器不能为空");
        }
        String[] levels = split(filter);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            int hash = level.indexOf('#');
            if (hash >= 0) {
                if (!"#".equals(level)) {
                    throw new InvalidTopicException(RejectReason.HASH_NOT_ALONE_IN_LEVEL,
                            "'#' 必须独占整个层级: " + filter);
                }
                if (i != levels.length - 1) {
                    throw new InvalidTopicException(RejectReason.HASH_NOT_AT_END,
                            "'#' 只能位于过滤器最后一层: " + filter);
                }
            }
            int plus = level.indexOf('+');
            if (plus >= 0 && !"+".equals(level)) {
                throw new InvalidTopicException(RejectReason.ILLEGAL_LEVEL_CHARACTER,
                        "'+' 必须独占整个层级: " + filter);
            }
        }
        return filter;
    }

    /**
     * 判断已校验的过滤器是否匹配已校验的具体主题。
     */
    public static boolean matches(String filter, String topic) {
        String[] f = split(filter);
        String[] t = split(topic);
        int fi = 0;
        int ti = 0;
        while (fi < f.length) {
            String fl = f[fi];
            if ("#".equals(fl)) {
                // '#' 匹配剩余零或多个层级
                return true;
            }
            if (ti >= t.length) {
                return false;
            }
            if ("+".equals(fl)) {
                // 匹配恰好一个层级（含空层级）
            } else if (!fl.equals(t[ti])) {
                return false;
            }
            fi++;
            ti++;
        }
        return ti == t.length;
    }

    private static String[] split(String s) {
        // limit=-1 保留结尾空字符串，使 "a/" 与 "a" 区分
        return s.split("/", -1);
    }
}
