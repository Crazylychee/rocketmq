/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.auth.authorization.chain;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.enums.UserStatus;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.Handler;
import org.apache.rocketmq.common.chain.HandlerChain;

public class UserAuthorizationHandler implements Handler<DefaultAuthorizationContext, CompletableFuture<Void>> {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    public UserAuthorizationHandler(AuthConfig config, Supplier<?> metadataService) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(config, metadataService);
    }

    //主要功能是根据授权上下文中的主体类型和用户类型进行不同的处理
    @Override
    public CompletableFuture<Void> handle(DefaultAuthorizationContext context, HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> chain) {
        if (!context.getSubject().isSubject(SubjectType.USER)) {
            return chain.handle(context);
        }
        return this.getUser(context.getSubject()).thenCompose(user -> {
            //如果用户类型是超级用户（UserType.SUPER），返回一个已完成的 CompletableFuture，表示不需要进一步处理。
            if (user.getUserType() == UserType.SUPER) {
                return CompletableFuture.completedFuture(null);
            }
            System.out.println(user);
            //如果用户类型不是超级用户，则将用户信息设置到授权上下文中，并继续处理链中的下一个处理器
            return chain.handle(context);
        });
    }

    private CompletableFuture<User> getUser(Subject subject) {
        if (this.authenticationMetadataProvider == null) {
            throw new AuthorizationException("The authenticationMetadataProvider is not configured");
        }
        User user = (User) subject;
        return authenticationMetadataProvider.getUser(user.getUsername()).thenApply(result -> {
            if (result == null) {
                throw new AuthorizationException("User:{} not found.", user.getUsername());
            }
            if (user.getUserStatus() == UserStatus.DISABLE) {
                throw new AuthenticationException("User:{} is disabled.", user.getUsername());
            }
            return result;
        });
    }
}
