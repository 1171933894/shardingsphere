package com.dangdang.ddframe.rdb.sharding.parsing.lexer.token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 词法标记.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
public final class Token {
    
    private final TokenType type;// 词法标记类型
    
    private final String literals;// 词法字面量标记
    
    private final int endPosition;// literals 在 SQL 里的结束位置
}
