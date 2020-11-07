/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.transaction.soft.api;

import com.dangdang.ddframe.rdb.sharding.executor.threadlocal.ExecutorExceptionHandler;
import com.dangdang.ddframe.rdb.sharding.jdbc.core.connection.ShardingConnection;
import com.dangdang.ddframe.rdb.transaction.soft.constants.SoftTransactionType;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/**
 * 柔性事务抽象类.
 *
 * 在 Sharding-JDBC 里，目前柔性事务分成两种：
 *
 *      BEDSoftTransaction ：最大努力送达型柔性事务
 *      TCCSoftTransaction ：TCC型柔性事务
 *
 * 柔性事务的适用场景：
 *
 *      根据主键删除数据。
 *      更新记录永久状态，如更新通知送达状态。
 *
 * 而且它还有一定的限制： SQL需要满足幂等性，具体为：
 *
 *      INSERT语句要求必须包含主键，且不能是自增主键。
 *      UPDATE语句要求幂等，不能是UPDATE xxx SET x=x+1
 *      DELETE语句无要求。
 *
 * 继承 AbstractSoftTransaction
 * 
 * @author zhangliang 
 */
public abstract class AbstractSoftTransaction {
    /**
     * 分片连接原自动提交状态
     */
    private boolean previousAutoCommit;
    /**
     * 分片连接
     */
    @Getter
    private ShardingConnection connection;
    /**
     * 事务类型
     */
    @Getter
    private SoftTransactionType transactionType;
    /**
     * 事务编号
     */
    @Getter
    private String transactionId;

    /**
     * 开启柔性
     *
     * @param conn 分片连接
     * @param type 事务类型
     * @throws SQLException
     */
    protected final void beginInternal(final Connection conn, final SoftTransactionType type) throws SQLException {
        // TODO 判断如果在传统事务中，则抛异常
        Preconditions.checkArgument(conn instanceof ShardingConnection, "Only ShardingConnection can support eventual consistency transaction.");
        // 设置执行错误，不抛出异常
        /**
         * 调用 ExecutorExceptionHandler.setExceptionThrown(false) 设置执行 SQL 错误时，也不抛出异常。
         *
         *      对异常处理的代码：ExecutorExceptionHandler#setExceptionThrown()
         *      对于其他 SQL，不会因为 SQL 错误不执行，会继续执行
         *      对于上层业务，不会因为 SQL 错误终止逻辑，会继续执行。这里有一点要注意下，上层业务不能对该 SQL 执行结果有强依赖，因为 SQL 错误需要重试达到数据最终一致性
         *      对于最大努力型事务( TCC暂未实现 )，会对执行错误的 SQL 进行重试
         */
        ExecutorExceptionHandler.setExceptionThrown(false);
        connection = (ShardingConnection) conn;
        transactionType = type;
        // 设置自动提交状态
        previousAutoCommit = connection.getAutoCommit();
        /**
         * 调用 connection.setAutoCommit(true);，设置执行自动提交。使用最大努力型事务时，上层业务执行 SQL 会马上提交，即使调用 Connection#rollback() 也是无法回滚的，这点一定要注意。
         */
        connection.setAutoCommit(true);
        // 生成事务编号
        // TODO 替换UUID为更有效率的id生成器
        transactionId = UUID.randomUUID().toString();
    }
    
    /**
     * 结束柔性事务.
     */
    public final void end() throws SQLException {
        if (connection != null) {
            ExecutorExceptionHandler.setExceptionThrown(true);
            connection.setAutoCommit(previousAutoCommit);
            SoftTransactionManager.closeCurrentTransactionManager();
        }
    }
}
