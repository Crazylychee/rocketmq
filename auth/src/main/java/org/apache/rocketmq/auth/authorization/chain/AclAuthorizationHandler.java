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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.Handler;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
//授权
public class AclAuthorizationHandler implements Handler<DefaultAuthorizationContext, CompletableFuture<Void>> {

    private final AuthorizationMetadataProvider authorizationMetadataProvider;

    public AclAuthorizationHandler(AuthConfig config) {
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(config);
    }
    /**
     * 构造函数，使用 AuthConfig 配置初始化授权元数据提供者
     * @param config 认证配置
     */
    public AclAuthorizationHandler(AuthConfig config, Supplier<?> metadataService) {
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(config, metadataService);
    }

    @Override
    public CompletableFuture<Void> handle(DefaultAuthorizationContext context,
        HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> chain) {
        if (this.authorizationMetadataProvider == null) {
            throw new AuthorizationException("The authorizationMetadataProvider is not configured");
        }
        return this.authorizationMetadataProvider.getAcl(context.getSubject()).thenAccept(acl -> {
            if (acl == null) {
                System.out.println("acl is null, skip authorization.");
                throwException(context, "no matched policies.");
            }

            // 1. get the defined acl entries which match the request.
            PolicyEntry matchedEntry = matchPolicyEntries(context, acl);

            // 如果没有匹配的策略条目，抛出一个异常，提示没有匹配的策略。也就是拒绝
            if (matchedEntry == null) {
                System.out.println("no matched policies.");
                throwException(context, "no matched policies.");
            }

            // 检查决策是否为拒绝
            if (matchedEntry.getDecision() == Decision.DENY) {
                System.out.println("the decision is deny.");
                throwException(context, "the decision is deny.");
            }
            System.out.println("the decision is allow, matched entry: " + matchedEntry);
        });
    }

    private PolicyEntry matchPolicyEntries(DefaultAuthorizationContext context, Acl acl) {
        List<PolicyEntry> policyEntries = new ArrayList<>();

        Policy policy = acl.getPolicy(PolicyType.CUSTOM);
        if (policy != null) {
            List<PolicyEntry> entries = matchPolicyEntries(context, policy.getEntries());
            if (CollectionUtils.isNotEmpty(entries)) {
                policyEntries.addAll(entries);
            }
        }

        if (CollectionUtils.isEmpty(policyEntries)) {
            policy = acl.getPolicy(PolicyType.DEFAULT);
            if (policy != null) {
                List<PolicyEntry> entries = matchPolicyEntries(context, policy.getEntries());
                if (CollectionUtils.isNotEmpty(entries)) {
                    policyEntries.addAll(entries);
                }
            }
        }

        if (CollectionUtils.isEmpty(policyEntries)) {
            return null;
        }

        policyEntries.sort(this::comparePolicyEntries);

        return policyEntries.get(0);
    }

    private List<PolicyEntry> matchPolicyEntries(DefaultAuthorizationContext context, List<PolicyEntry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return null;
        }
        return entries.stream()
            .filter(entry -> entry.isMatchResource(context.getResource()))
            .filter(entry -> entry.isMatchAction(context.getActions()))
            .filter(entry -> entry.isMatchEnvironment(Environment.of(context.getSourceIp())))
            .collect(Collectors.toList());
    }

    private int comparePolicyEntries(PolicyEntry o1, PolicyEntry o2) {
        int compare = 0;
        Resource r1 = o1.getResource();
        Resource r2 = o2.getResource();
        if (r1.getResourceType() != r2.getResourceType()) {
            if (r1.getResourceType() == ResourceType.ANY) {
                compare = 1;
            }
            if (r2.getResourceType() == ResourceType.ANY) {
                compare = -1;
            }
        } else if (r1.getResourcePattern() == r2.getResourcePattern()) {
            if (r1.getResourcePattern() == ResourcePattern.PREFIXED) {
                String n1 = r1.getResourceName();
                String n2 = r2.getResourceName();
                compare = Integer.compare(n1.length(), n2.length());
            }
        } else {
            if (r1.getResourcePattern() == ResourcePattern.LITERAL) {
                compare = 1;
            }
            if (r1.getResourcePattern() == ResourcePattern.LITERAL) {
                compare = -1;
            }
            if (r1.getResourcePattern() == ResourcePattern.PREFIXED) {
                compare = 1;
            }
            if (r1.getResourcePattern() == ResourcePattern.PREFIXED) {
                compare = -1;
            }
        }

        if (compare != 0) {
            return compare;
        }

        // the decision deny has higher priority
        Decision d1 = o1.getDecision();
        Decision d2 = o2.getDecision();
        return d1 == Decision.DENY ? 1 : d2 == Decision.DENY ? -1 : 0;
    }

    private static void throwException(DefaultAuthorizationContext context, String detail) {
        throw new AuthorizationException("{} has no permission to access {} from {}, " + detail,
            context.getSubject().getSubjectKey(), context.getResource().getResourceKey(), context.getSourceIp());
    }
}
