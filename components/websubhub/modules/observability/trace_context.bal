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

import ballerina/jballerina.java;
import ballerina/log;
import ballerina/observe;

import wso2/messagestore.api as storeapi;

const string PROPERTY_TRACE_PROPERTIES = "_trace_properties_";
const string PROPERTY_ERROR_VALUE = "_ballerina_error_value_";
const string RECEIVE_DURATION_TAG = "hub.receive.duration_seconds";

const string DELIVERY_SPAN_NAME = "wso2/websubhub.delivery/consume";

const string TRACEPARENT_FIELD = "traceparent";
const string TRACESTATE_FIELD = "tracestate";

# An open delivery span, together with the observer context it displaced.
#
# + context - The `ObserverContext` of the span this module started
# + previous - The observer context installed before it, restored when the span finishes
public type ConsumeSpan record {|
    handle context;
    handle previous;
|};

# Opens the delivery span, and contains any failure in doing so.
#
# + message - The message about to be delivered
# + receiveDuration - How long the `receive` that produced this message took
# + return - The started span, or `()` if none was started or tracing failed
public isolated function startConsumeSpan(storeapi:Message message, decimal receiveDuration) returns ConsumeSpan? {
    ConsumeSpan|error? span = trap openConsumeSpan(message, receiveDuration);
    if span is error {
        logTracingFailure("Failed to open the delivery span in the publisher's trace", span);
        return;
    }
    return span;
}

# Finishes the delivery span, and contains any failure in doing so.
#
# + span - The value returned by `startConsumeSpan`, or `()` when no span was started
# + err - The delivery failure to record on the span, or `()` when the delivery succeeded
public isolated function finishConsumeSpan(ConsumeSpan? span, error? err = ()) {
    if span is () {
        return;
    }
    closeConsumeSpan(span, err);
    error? restored = trap setObserverContext(span.previous);
    if restored is error {
        logTracingFailure("Failed to restore the observer context after delivery", restored);
    }
}

isolated function logTracingFailure(string message, error err) {
    log:printWarn(message, 'error = err);
}

isolated function openConsumeSpan(storeapi:Message message, decimal receiveDuration) returns ConsumeSpan|error? {
    map<string> traceContext = extractTraceContext(message);
    if traceContext.length() == 0 {
        return;
    }
    handle carrier = newHashMap();
    foreach [string, string] [key, value] in traceContext.entries() {
        _ = mapPut(carrier, java:fromString(key), java:fromString(value));
    }

    handle previous = getObserverContext();
    handle context = newObserverContext();
    setOperationName(context, java:fromString(DELIVERY_SPAN_NAME));
    addProperty(context, java:fromString(PROPERTY_TRACE_PROPERTIES), carrier);
    if !java:isNull(previous) {
        setServiceName(context, getServiceName(previous));
    }
    setObserverContext(context);
    error? started = trap startObservation(context, true);
    if started is error {
        error? restored = trap setObserverContext(previous);
        if restored is error {
            logTracingFailure("Failed to restore the observer context after failing to open the delivery span",
                    restored);
        }
        return started;
    }
    addDeliveryTags(previous, message, receiveDuration);
    return {context, previous};
}

isolated function addDeliveryTags(handle previous, storeapi:Message message, decimal receiveDuration) {
    map<string> attributes = message.receiveAttributes ?: {};
    foreach [string, string] [name, value] in attributes.entries() {
        addSpanTag(name, value);
    }
    if !java:isNull(previous) {
        string? entrypointModule = java:toString(getEntrypointFunctionModule(previous));
        if entrypointModule is string {
            addSpanTag("entrypoint.function.module", entrypointModule);
        }
        string? entrypointFunction = java:toString(getEntrypointFunctionName(previous));
        if entrypointFunction is string {
            addSpanTag("entrypoint.function.name", entrypointFunction);
        }
    }
    addSpanTag(RECEIVE_DURATION_TAG, receiveDuration.toString());
}

isolated function addSpanTag(string name, string value) {
    error? tagged = observe:addTagToSpan(name, value);
    if tagged is error {
        logTracingFailure("Failed to tag the delivery span with " + name, tagged);
    }
}

isolated function closeConsumeSpan(ConsumeSpan span, error? err) {
    if err is error {
        error? reported = trap reportError(err);
        if reported is error {
            logTracingFailure("Failed to record the delivery failure on the delivery span", reported);
        }
    }
    error? stopped = trap stopObservation(span.context);
    if stopped is error {
        logTracingFailure("Failed to stop the delivery span", stopped);
    }
}

isolated function extractTraceContext(storeapi:Message message) returns map<string> {
    map<string|string[]>? metadata = message.metadata;
    if metadata is () {
        return {};
    }
    string|string[]? traceParent = metadata[TRACEPARENT_FIELD];
    if traceParent !is string {
        return {};
    }
    map<string> traceContext = {[TRACEPARENT_FIELD]: traceParent};
    string|string[]? traceState = metadata[TRACESTATE_FIELD];
    if traceState is string {
        traceContext[TRACESTATE_FIELD] = traceState;
    }
    return traceContext;
}

isolated function newHashMap() returns handle = @java:Constructor {
    'class: "java.util.HashMap"
} external;

isolated function mapPut(handle target, handle key, handle value) returns handle = @java:Method {
    name: "put",
    'class: "java.util.Map"
} external;

isolated function newObserverContext() returns handle = @java:Constructor {
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function addProperty(handle context, handle key, handle value) = @java:Method {
    name: "addProperty",
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function reportError(error value) = @java:Method {
    name: "reportError",
    'class: "io.ballerina.runtime.observability.ObserveUtils"
} external;

isolated function setOperationName(handle context, handle name) = @java:Method {
    name: "setOperationName",
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function getServiceName(handle context) returns handle = @java:Method {
    name: "getServiceName",
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function getEntrypointFunctionModule(handle context) returns handle = @java:Method {
    name: "getEntrypointFunctionModule",
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function getEntrypointFunctionName(handle context) returns handle = @java:Method {
    name: "getEntrypointFunctionName",
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function setServiceName(handle context, handle serviceName) = @java:Method {
    name: "setServiceName",
    'class: "io.ballerina.runtime.observability.ObserverContext"
} external;

isolated function getObserverContext() returns handle = @java:Method {
    name: "getObserverContextOfCurrentFrame",
    'class: "io.ballerina.runtime.observability.ObserveUtils"
} external;

isolated function setObserverContext(handle context) = @java:Method {
    name: "setObserverContextToCurrentFrame",
    'class: "io.ballerina.runtime.observability.ObserveUtils"
} external;

isolated function startObservation(handle context, boolean isClient) = @java:Method {
    name: "startObservation",
    'class: "io.ballerina.runtime.observability.tracer.TracingUtils"
} external;

isolated function stopObservation(handle context) = @java:Method {
    name: "stopObservation",
    'class: "io.ballerina.runtime.observability.tracer.TracingUtils"
} external;
