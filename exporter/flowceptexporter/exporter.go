// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptexporter // import "github.com/valescamoura/opentelemetry-collector/exporter/flowceptexporter"

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// pendingTrace holds the spans of a trace whose root span was not seen yet.
// Child spans end (and are exported) before their root, and the root is the
// span that carries the session, so a trace is written once its root arrives.
type pendingTrace struct {
	spans     []spanRecord
	firstSeen time.Time
}

// resolvedTrace remembers where a written trace went, so spans that arrive
// after the root are written to the same workflow and campaign.
type resolvedTrace struct {
	workflowID string
	campaignID string
	resolvedAt time.Time
}

type flowceptExporter struct {
	cfg    *Config
	logger *zap.Logger
	now    func() time.Time

	mu        sync.Mutex
	pending   map[string]*pendingTrace
	resolved  map[string]resolvedTrace
	workflows map[string]bool
	file      *os.File

	stop chan struct{}
	done chan struct{}
}

func newFlowceptExporter(cfg *Config, logger *zap.Logger) *flowceptExporter {
	return &flowceptExporter{
		cfg:       cfg,
		logger:    logger,
		now:       time.Now,
		pending:   map[string]*pendingTrace{},
		resolved:  map[string]resolvedTrace{},
		workflows: map[string]bool{},
	}
}

func (e *flowceptExporter) start(_ context.Context, _ component.Host) error {
	file, err := os.OpenFile(e.cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", e.cfg.Path, err)
	}
	e.file = file
	e.stop = make(chan struct{})
	e.done = make(chan struct{})
	go e.sweepLoop()
	return nil
}

func (e *flowceptExporter) shutdown(_ context.Context) error {
	if e.file == nil {
		return nil
	}
	close(e.stop)
	<-e.done

	e.mu.Lock()
	defer e.mu.Unlock()
	for traceID := range e.pending {
		if err := e.flushTraceLocked(traceID); err != nil {
			e.logger.Error("failed to flush trace on shutdown", zap.String("trace_id", traceID), zap.Error(err))
		}
	}
	err := e.file.Close()
	e.file = nil
	return err
}

func (e *flowceptExporter) pushTraces(_ context.Context, td ptrace.Traces) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var rootTraces []string
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				rec := newSpanRecord(spans.At(k), rs.Resource(), ss.Scope())
				if done, ok := e.resolved[rec.traceID]; ok {
					if err := e.writeLocked(taskRecord(rec, done.workflowID, done.campaignID, e.cfg.AdapterID, e.nowSeconds())); err != nil {
						return err
					}
					continue
				}
				p, ok := e.pending[rec.traceID]
				if !ok {
					p = &pendingTrace{firstSeen: e.now()}
					e.pending[rec.traceID] = p
				}
				p.spans = append(p.spans, rec)
				if rec.isRoot() {
					rootTraces = append(rootTraces, rec.traceID)
				}
			}
		}
	}

	for _, traceID := range rootTraces {
		if err := e.flushTraceLocked(traceID); err != nil {
			return err
		}
	}
	return nil
}

// flushTraceLocked writes all held spans of a trace. Callers hold e.mu.
func (e *flowceptExporter) flushTraceLocked(traceID string) error {
	p, ok := e.pending[traceID]
	if !ok {
		return nil
	}
	delete(e.pending, traceID)

	campaignID := sessionOf(p.spans)
	workflowID := workflowIDFor(e.cfg.WorkflowIDPrefix, campaignID, traceID)
	now := e.nowSeconds()

	if campaignID != "" && !e.workflows[workflowID] {
		if err := e.writeLocked(workflowRecord(workflowID, campaignID, e.cfg.AdapterID, now)); err != nil {
			return err
		}
		e.workflows[workflowID] = true
	}
	for _, rec := range p.spans {
		if err := e.writeLocked(taskRecord(rec, workflowID, campaignID, e.cfg.AdapterID, now)); err != nil {
			return err
		}
	}
	e.resolved[traceID] = resolvedTrace{workflowID: workflowID, campaignID: campaignID, resolvedAt: e.now()}
	return nil
}

func (e *flowceptExporter) writeLocked(record map[string]any) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}
	line = append(line, '\n')
	if _, err := e.file.Write(line); err != nil {
		return fmt.Errorf("failed to write %q: %w", e.cfg.Path, err)
	}
	return nil
}

// sweepLoop writes traces whose root never arrived and forgets old resolved
// traces, both after TraceTimeout.
func (e *flowceptExporter) sweepLoop() {
	defer close(e.done)
	ticker := time.NewTicker(e.cfg.TraceTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-ticker.C:
			e.sweep()
		}
	}
}

func (e *flowceptExporter) sweep() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	for traceID, p := range e.pending {
		if now.Sub(p.firstSeen) >= e.cfg.TraceTimeout {
			if err := e.flushTraceLocked(traceID); err != nil {
				e.logger.Error("failed to flush timed out trace", zap.String("trace_id", traceID), zap.Error(err))
			}
		}
	}
	for traceID, r := range e.resolved {
		if now.Sub(r.resolvedAt) >= e.cfg.TraceTimeout {
			delete(e.resolved, traceID)
		}
	}
}

func (e *flowceptExporter) nowSeconds() float64 {
	return float64(e.now().UnixNano()) / float64(time.Second)
}
