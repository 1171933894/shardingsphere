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

package com.dangdang.ddframe.rdb.sharding.jdbc.adapter;

import com.dangdang.ddframe.rdb.sharding.jdbc.unsupported.AbstractUnsupportedOperationConnection;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collection;
import java.util.LinkedList;

/**
 * 数据库连接适配类.
 * 
 * @author zhangliang
 */
public abstract class AbstractConnectionAdapter extends AbstractUnsupportedOperationConnection {
    /**
     * 是否自动提交
     */
    private boolean autoCommit = true;
    
    private boolean readOnly = true;
    
    private boolean closed;
    
    private int transactionIsolation = TRANSACTION_READ_UNCOMMITTED;
    /**
     * 获得链接 和分库分表相关，因而仅抽象该方法，留给子类实现
     *
     * @return 链接
     */
    protected abstract Collection<Connection> getConnections();
    
    @Override
    public final boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    /**
     * 调用时，实际会设置其所持有的 Connection 的 autoCommit 属性
     */
    @Override
    public final void setAutoCommit(final boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        if (getConnections().isEmpty()) {// 无数据连接时，记录方法调用
            recordMethodInvocation(Connection.class, "setAutoCommit", new Class[] {boolean.class}, new Object[] {autoCommit});
            return;
        }
        for (Connection each : getConnections()) {
            each.setAutoCommit(autoCommit);
        }
    }

    /**
     * 引擎会遍历底层所有真正的数据库连接，一个个进行commit操作，如果任何一个出现了异常，
     * 直接捕获异常，但是也只是捕获而已，然后接着下一个连接的commit，这也就很好的说明了，
     * 如果在执行任何一条sql语句出现了异常，整个操作是可以原子性回滚的，因为此时所有连接
     * 都不会执行commit，但如果已经到了commit这一步的话，如果有连接commit失败了，是不
     * 会影响到其他连接的。
     */
    // sharding-jdbc默认是如何处理事务的？看看这个方法代码
    @Override
    public final void commit() throws SQLException {
        for (Connection each : getConnections()) {
            each.commit();
        }
    }
    
    @Override
    public final void rollback() throws SQLException {
        Collection<SQLException> exceptions = new LinkedList<>();
        for (Connection each : getConnections()) {
            try {
                each.rollback();
            } catch (final SQLException ex) {
                exceptions.add(ex);
            }
        }
        throwSQLExceptionIfNessesary(exceptions);
    }
    
    @Override
    public void close() throws SQLException {
        closed = true;
        MetricsContext.clear();
        Collection<SQLException> exceptions = new LinkedList<>();
        for (Connection each : getConnections()) {
            try {
                each.close();
            } catch (final SQLException ex) {
                exceptions.add(ex);
            }
        }
        throwSQLExceptionIfNessesary(exceptions);
    }
    
    @Override
    public final boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public final boolean isReadOnly() throws SQLException {
        return readOnly;
    }
    
    @Override
    public final void setReadOnly(final boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
        if (getConnections().isEmpty()) {
            recordMethodInvocation(Connection.class, "setReadOnly", new Class[] {boolean.class}, new Object[] {readOnly});
            return;
        }
        for (Connection each : getConnections()) {
            each.setReadOnly(readOnly);
        }
    }
    
    @Override
    public final int getTransactionIsolation() throws SQLException {
        return transactionIsolation;
    }
    
    @Override
    public final void setTransactionIsolation(final int level) throws SQLException {
        transactionIsolation = level;
        if (getConnections().isEmpty()) {
            recordMethodInvocation(Connection.class, "setTransactionIsolation", new Class[] {int.class}, new Object[] {level});
            return;
        }
        for (Connection each : getConnections()) {
            each.setTransactionIsolation(level);
        }
    }
    
    // -------以下代码与MySQL实现保持一致.-------
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
    }
    
    @Override
    public final int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public final void setHoldability(final int holdability) throws SQLException {
    }
}