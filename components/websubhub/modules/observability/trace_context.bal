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

import wso2/messagestore.api as storeapi;

const string PROPERTY_TRACE_PROPERTIES = "_trace_properties_";
const string PROPERTY_ERROR_VALUE = "_ballerina_error_value_";
const string STAND_IN_TAG = "hub.span.stand_in";
const string RECEIVE_DURATION_TAG = "hub.receive.duration_seconds";

const string POLLING_TRACE_TAG = "hub.polling.trace_id";
const string CONNECTOR_RECEIVE_SPAN_NAME = "xlibb/solace/MessageConsumer:receive";
const string RECEIVE_SRC_MODULE = "wso2/messagestore.solace";
const string RECEIVE_SRC_OBJECT = "xlibb/solace/MessageConsumer";
const string RECEIVE_SRC_FUNCTION = "receive";

final readonly & string[] TRACE_CONTEXT_FIELDS = ["traceparent", "tracestate"];

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
# This sits on the message-delivery path: it runs after a message has been consumed but before it has
# been delivered, so a fault escaping here would fail the delivery and lose the message. Everything
# below reaches into runtime internals through interop, where a runtime upgrade can turn a working
# binding into a runtime panic - so the whole thing is trapped. Tracing must never affect delivery.
#
# + message - The message about to be delivered
# + receiveDuration - How long the `receive` that produced this message took
# + return - The started span, or `()` if none was started or tracing failed
public isolated function startConsumeSpan(storeapi:Message message, decimal receiveDuration) returns ConsumeSpan? {
    ConsumeSpan?|error span = trap openConsumeSpan(message, receiveDuration);
    if span is error {
        logTracingFailure("Failed to open the delivery span in the publisher's trace", span);
        return;
    }
    return span;
}

# Finishes the delivery span, and contains any failure in doing so.
#
# A failure here would otherwise leave the span open and the displaced observer context installed, so
# it also restores the ambient context on the way out - see `closeConsumeSpan`.
#
# + span - The value returned by `startConsumeSpan`, or `()` when no span was started
# + err - The delivery failure to record on the span, or `()` when the delivery succeeded
public isolated function finishConsumeSpan(ConsumeSpan? span, error? err = ()) {
    if span is () {
        return;
    }
    error? closed = trap closeConsumeSpan(span, err);
    if closed is error {
        logTracingFailure("Failed to close the delivery span in the publisher's trace", closed);
    }
    error? restored = trap setObserverContext(span.previous);
    if restored is error {
        logTracingFailure("Failed to restore the observer context after delivery", restored);
    }
}

# Reports a tracing fault. Logged every time rather than once: delivery keeps working, so repeated
# warnings are the only signal that the trace has quietly stopped joining up.
#
# + message - What failed
# + err - The underlying failure
isolated function logTracingFailure(string message, error err) {
    log:printWarn(message, 'error = err);
}

isolated function openConsumeSpan(storeapi:Message message, decimal receiveDuration) returns ConsumeSpan? {
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
    setOperationName(context, java:fromString(CONNECTOR_RECEIVE_SPAN_NAME));
    addProperty(context, java:fromString(PROPERTY_TRACE_PROPERTIES), carrier);
    if !java:isNull(previous) {
        setServiceName(context, getServiceName(previous));
        tagPollingTrace(context, previous);
    }
    addReceiveTags(context, previous, message, receiveDuration);
    setObserverContext(context);
    startObservation(context, true);
    return {context, previous};
}

isolated function addReceiveTags(handle context, handle previous, storeapi:Message message,
        decimal receiveDuration) {
    map<string> attributes = message.receiveAttributes ?: {};
    foreach [string, string] [name, value] in attributes.entries() {
        addTag(context, java:fromString(name), java:fromString(value));
    }
    addTag(context, java:fromString("src.client.remote"), java:fromString("true"));
    addTag(context, java:fromString("src.object.name"), java:fromString(RECEIVE_SRC_OBJECT));
    addTag(context, java:fromString("src.function.name"), java:fromString(RECEIVE_SRC_FUNCTION));
    string srcModule = RECEIVE_SRC_MODULE;
    if !java:isNull(previous) {
        string? entrypointModule = java:toString(getEntrypointFunctionModule(previous));
        string? entrypointFunction = java:toString(getEntrypointFunctionName(previous));
        if entrypointModule is string {
            addTag(context, java:fromString("entrypoint.function.module"), java:fromString(entrypointModule));
            int? separator = entrypointModule.indexOf(":");
            if separator is int {
                srcModule += entrypointModule.substring(separator);
            }
        }
        if entrypointFunction is string {
            addTag(context, java:fromString("entrypoint.function.name"), java:fromString(entrypointFunction));
        }
    }
    addTag(context, java:fromString("src.module"), java:fromString(srcModule));
    addTag(context, java:fromString(RECEIVE_DURATION_TAG), java:fromString(receiveDuration.toString()));
    addTag(context, java:fromString(STAND_IN_TAG), java:fromString("true"));
}

isolated function tagPollingTrace(handle context, handle previous) {
    string? traceParent = java:toString(mapGet(getContextProperties(previous), java:fromString("traceparent")));
    if traceParent is string && traceParent.length() >= 35 {
        addTag(context, java:fromString(POLLING_TRACE_TAG), java:fromString(traceParent.substring(3, 35)));
    }
}
isolated function closeConsumeSpan(ConsumeSpan span, error? err) {
    if err is error {
        reportError(err);
    }
    stopObservation(span.context);
}

isolated function extractTraceContext(storeapi:Message message) returns map<string> {
    map<string> traceContext = {};
    map<string|string[]>? metadata = message.metadata;
    if metadata is () {
        return traceContext;
    }
    foreach string name in TRACE_CONTEXT_FIELDS {
        string|string[]? value = metadata[name];
        if value is string {
            traceContext[name] = value;
        }
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

isolated function mapGet(handle target, handle key) returns handle = @java:Method {
    name: "get",
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

isolated function addTag(handle context, handle key, handle value) = @java:Method {
    name: "addTag",
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

isolated function getContextProperties(handle context) returns handle = @java:Method {
    name: "getContextProperties",
    'class: "io.ballerina.runtime.observability.ObserveUtils"
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
