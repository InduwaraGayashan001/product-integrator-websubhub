// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
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

import websubhub.common;
import websubhub.state;

import ballerina/websubhub;

import wso2/messagestore.api as storeapi;

# Constructs the content-distribution notification for a message consumed from the message store.
#
# The content type is resolved from the hub's replicated topic state rather than travelling with
# the message, because message-store metadata does not round-trip on the supported brokers.
#
# + topic - The topic the message was published to
# + message - The message consumed from the message store
# + return - The notification to deliver to the subscriber, or an `error` if it cannot be built
isolated function constructContentDistMsg(string topic, storeapi:Message message) returns websubhub:ContentDistributionMessage|error {
    return {
        content: message.payload,
        contentType: state:getTopicContentType(topic),
        headers: constructDeliveryHeaders(message)
    };
}

# Derives the headers to send with a content-delivery request.
#
# + message - The message consumed from the message store
# + return - The headers to include in the content-delivery request, or `()` if there are none
isolated function constructDeliveryHeaders(storeapi:Message message) returns map<string|string[]>? {
    map<string|string[]> deliveryHeaders = {};
    map<string|string[]>? metadata = message.metadata;
    if metadata is map<string|string[]> {
        foreach var [headerName, headerValue] in metadata.entries() {
            if common:isDeniedMetadataHeader(headerName) {
                continue;
            }
            deliveryHeaders[headerName] = headerValue;
        }
    }

    string? messageId = message.id;
    if messageId is string {
        deliveryHeaders[common:MESSAGE_ID_HEADER] = messageId;
    }
    return deliveryHeaders.length() == 0 ? () : deliveryHeaders;
}
