// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;
import ballerina/test;
import ballerina/url;
import ballerina/uuid;

// Starting the content delivery of a subscription, which begins with binding a consumer to the queue of the
// subscription. A test makes the bind fail by giving the subscription a queue of its own, through the
// `solace.queue_name` parameter, which it creates beforehand without granting the hub access to it. Every attempt of
// the hub to start the delivery is a bind request on that queue, which the broker counts. Whether the hub marked the
// subscription as stale shows in whether it lets the subscription be renewed, as it denies subscribing again to an
// active subscription.

// The `delivery.startRetries` of resources/hub.toml. The hub of resources/hub-no-retries.toml does not retry.
const int START_RETRIES = 10;
const string QUEUE_NAME_PARAM = "solace.queue_name";
// The owner of the queues the tests create, so that the hub, which connects as 'admin', is not the owner
const string QUEUE_OWNER = "websubhub-it-queue-owner";

@test:Config {groups: ["delivery-start"]}
function testDeliveryStartRecoversWithinRetries() returns error? {
    final string topic = newTopic();
    final string id = uuid:createType4AsString();
    final string callback = callbackUrl(id);
    final string queue = check createInaccessibleQueue(id);
    check registerAndWaitForTopic(topic);

    http:Response response = check subscribe(topic, callback, extraParams = {[QUEUE_NAME_PARAM]: queue});
    test:assertEquals(response.statusCode, http:STATUS_ACCEPTED, "Subscription should be accepted");
    check waitUntil(isolated function() returns boolean|error => check bindRequestsOf(queue) > 0,
            "the hub to attempt to start the content delivery");
    check grantConsumeAccess(queue);

    check waitUntil(isolated function() returns boolean|error => publishedAndDelivered(topic, id, 0),
            "the content to be delivered once the hub is granted access to the queue");
    test:assertTrue(check bindRequestsOf(queue) > 1, "The hub should have retried starting the content delivery");
    test:assertFalse(check renewSubscription(topic, callback, id),
            "A subscription whose content delivery started within the retries should stay active");
}

@test:Config {groups: ["delivery-start"]}
function testDeliveryStartMarksSubscriptionStaleOnceRetriesAreExhausted() returns error? {
    final string topic = newTopic();
    final string id = uuid:createType4AsString();
    final string callback = callbackUrl(id);
    final string queue = check createInaccessibleQueue(id);
    check registerAndWaitForTopic(topic);

    http:Response response = check subscribe(topic, callback, extraParams = {[QUEUE_NAME_PARAM]: queue});
    test:assertEquals(response.statusCode, http:STATUS_ACCEPTED, "Subscription should be accepted");

    check assertStartAttempts(queue, START_RETRIES + 1);
    check waitUntil(isolated function() returns boolean|error => renewSubscription(topic, callback, id),
            "the subscription to be marked as stale");
}

@test:Config {groups: ["delivery-start"]}
function testDeliveryStartWithoutRetries() returns error? {
    final string topic = newTopic();
    final string id = uuid:createType4AsString();
    final string callback = callbackUrl(id);
    final string queue = check createInaccessibleQueue(id);
    check registerAndWaitForTopic(topic);
    check waitUntil(isolated function() returns boolean|error {
        http:Response publishResponse = check publish(topic, {"event": "probe"}, hub = noRetriesHubClient);
        return publishResponse.statusCode == http:STATUS_OK;
    }, string `topic ${topic} to be registered in the hub without retries`);

    http:Response response = check subscribe(topic, callback, extraParams = {[QUEUE_NAME_PARAM]: queue},
            hub = noRetriesHubClient);
    test:assertEquals(response.statusCode, http:STATUS_ACCEPTED, "Subscription should be accepted");

    check assertStartAttempts(queue, 1);
    check waitUntil(isolated function() returns boolean|error => renewSubscription(topic, callback, id),
            "the subscription to be marked as stale");
}

// ========================================
// Helpers
// ========================================

# Asserts the number of attempts to start the content delivery of a subscription made through its queue. The broker
# updates its counters asynchronously, hence the count is first waited for.
#
# + queue - The queue of the subscription
# + expected - The expected number of attempts
# + return - An `error` if the count cannot be retrieved or does not reach the expected number in time
isolated function assertStartAttempts(string queue, int expected) returns error? {
    check waitUntil(isolated function() returns boolean|error => check bindRequestsOf(queue) >= expected,
            string `${expected} attempts to start the content delivery`);
    test:assertEquals(check bindRequestsOf(queue), expected,
            "Unexpected number of attempts to start the content delivery");
}

# Creates a queue for a subscription that the hub is not allowed to consume from.
#
# + id - The identifier of the test subscriber
# + return - The name of the queue, or an `error` if the broker rejects it
isolated function createInaccessibleQueue(string id) returns string|error {
    http:Response response = check brokerSempClient->post("/config/msgVpns/default/clientUsernames",
            {clientUsername: QUEUE_OWNER, enabled: false});
    if response.statusCode != http:STATUS_OK && !(check response.getTextPayload()).includes("ALREADY_EXISTS") {
        return error(string `Failed to create the client username ${QUEUE_OWNER}: ${check response.getTextPayload()}`);
    }
    string queue = string `websubhub-it-inaccessible-${id}`;
    response = check brokerSempClient->post("/config/msgVpns/default/queues", {
        queueName: queue,
        accessType: "non-exclusive",
        owner: QUEUE_OWNER,
        permission: "no-access",
        ingressEnabled: true,
        egressEnabled: true
    });
    if response.statusCode != http:STATUS_OK {
        return error(string `Failed to create the queue ${queue}: ${check response.getTextPayload()}`);
    }
    return queue;
}

isolated function grantConsumeAccess(string queue) returns error? {
    check updateQueue(queue, {permission: "consume"});
}

# Retrieves the number of consumer bind requests a queue of the broker received.
#
# + queue - The name of the queue
# + return - The number of bind requests, or an `error` if it cannot be retrieved
isolated function bindRequestsOf(string queue) returns int|error {
    http:Response response = check brokerSempClient->get(
            string `/monitor/msgVpns/default/queues/${check url:encode(queue, "UTF-8")}?select=bindRequestCount`);
    json body = check response.getJsonPayload();
    if response.statusCode != http:STATUS_OK {
        return error(string `Failed to retrieve the queue ${queue}: ${body.toJsonString()}`);
    }
    return (check body.data.bindRequestCount).ensureType();
}
