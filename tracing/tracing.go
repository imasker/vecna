package tracing

import (
	"encoding/json"

	"github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"

	"github.com/imasker/vecna/tasks"
)

// opentracing tags
var (
	VecnaTag         = opentracing.Tag{Key: string(opentracing_ext.Component), Value: "vecna"}
	WorkflowGroupTag = opentracing.Tag{Key: "vecna.workflow", Value: "group"}
	WorkflowChordTag = opentracing.Tag{Key: "vecna.workflow", Value: "chord"}
	WorkflowChainTag = opentracing.Tag{Key: "vecna.workflow", Value: "chain"}
)

func StartSpanFromHeaders(headers tasks.Headers, operationName string) opentracing.Span {
	// Try to extract the span context from the carrier.
	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, headers)

	// Create a new span from the span context if found or start a new trace with the function name.
	// For clarity add the vecna component tag.
	span := opentracing.StartSpan(operationName, ConsumerOption(spanContext), VecnaTag)

	// Log any error but don't fail
	if err != nil {
		span.LogFields(opentracing_log.Error(err))
	}

	return span
}

// HeadersWithSpan will inject a span into the signature headers
func HeadersWithSpan(headers tasks.Headers, span opentracing.Span) tasks.Headers {
	// check if the headers aren't nil
	if headers == nil {
		headers = make(tasks.Headers)
	}

	if err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, headers); err != nil {
		span.LogFields(opentracing_log.Error(err))
	}

	return headers
}

type consumerOption struct {
	producerContext opentracing.SpanContext
}

func (c consumerOption) Apply(o *opentracing.StartSpanOptions) {
	if c.producerContext != nil {
		opentracing.FollowsFrom(c.producerContext).Apply(o)
	}
	opentracing_ext.SpanKindConsumer.Apply(o)
}

// ConsumerOption ...
func ConsumerOption(producer opentracing.SpanContext) opentracing.StartSpanOption {
	return consumerOption{producer}
}

type producerOption struct{}

func (p producerOption) Apply(o *opentracing.StartSpanOptions) {
	opentracing_ext.SpanKindProducer.Apply(o)
}

// ProducerOption ...
func ProducerOption() opentracing.StartSpanOption {
	return producerOption{}
}

// AnnotateSpanWithSignatureInfo ...
func AnnotateSpanWithSignatureInfo(span opentracing.Span, signature *tasks.Signature) {
	// tag the span with some info about the signature
	span.SetTag("signature.name", signature.Name)
	span.SetTag("signature.id", signature.ID)

	if signature.GroupID != "" {
		span.SetTag("signature.group.id", signature.GroupID)
	}

	if signature.ChordCallback != nil {
		span.SetTag("signature.chord.callback.id", signature.ChordCallback.ID)
		span.SetTag("signature.chord.callback.name", signature.ChordCallback.Name)
	}
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(span opentracing.Span, chain *tasks.Chain) {
	// tag the span with some info about the chain
	span.SetTag("chain.tasks.length", len(chain.Tasks))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithGroupInfo ...
func AnnotateSpanWithGroupInfo(span opentracing.Span, group *tasks.Group, sendConcurrency int) {
	// tag the span with some info about the group
	span.SetTag("group.id", group.GroupID)
	span.SetTag("group.tasks.length", len(group.Tasks))
	span.SetTag("group.concurrency", sendConcurrency)

	// encode the task ids to json, if that fails just jump it in
	if taskIDs, err := json.Marshal(group.GetIDs()); err == nil {
		span.SetTag("group.tasks", string(taskIDs))
	} else {
		span.SetTag("group.tasks", group.GetIDs())
	}

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(span opentracing.Span, chord *tasks.Chord, sendConcurrency int) {
	// tag the span with chord specific info
	span.SetTag("chord.callback.id", chord.Callback.ID)

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithSpan(chord.Callback.Headers, span)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(span, chord.Group, sendConcurrency)
}
