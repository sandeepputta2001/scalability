// Saga Pattern — Orchestrator Style
// ───────────────────────────────────
// Manages a distributed transaction that spans multiple services/databases.
// Each step has a forward action and a compensating action (rollback).
//
// Why Sagas instead of 2PC (Two-Phase Commit)?
//
//	2PC requires all participants to hold locks during the prepare phase.
//	In a distributed system with N services, this causes:
//	  - Long lock hold times → low throughput
//	  - Coordinator failure → all participants blocked indefinitely
//	  - Not supported by most NoSQL databases
//
//	Sagas use local transactions + compensating transactions:
//	  - Each step commits locally (no distributed lock)
//	  - If a step fails, previous steps are compensated (rolled back)
//	  - No global lock, no coordinator bottleneck
//
// Order Placement Saga:
//
//	Step 1: Reserve stock (MongoDB)
//	  Compensate: Release stock
//	Step 2: Create order (PostgreSQL shard)
//	  Compensate: Cancel order
//	Step 3: Charge payment (external service)
//	  Compensate: Refund payment
//	Step 4: Confirm order (PostgreSQL shard)
//	  Compensate: Cancel order
//
// Tradeoff:
//
//	✓ No distributed locks, high throughput
//	✓ Works across heterogeneous databases
//	✗ Eventual consistency: intermediate states are visible
//	✗ Compensations must be idempotent (may run multiple times)
//	✗ Complex error handling: what if compensation also fails?
package patterns

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// StepStatus represents the execution state of a saga step.
type StepStatus string

const (
	StepPending     StepStatus = "pending"
	StepCompleted   StepStatus = "completed"
	StepFailed      StepStatus = "failed"
	StepCompensated StepStatus = "compensated"
)

// SagaStatus represents the overall saga state.
type SagaStatus string

const (
	SagaRunning    SagaStatus = "running"
	SagaCompleted  SagaStatus = "completed"
	SagaFailed     SagaStatus = "failed"      // compensation succeeded
	SagaCompFailed SagaStatus = "comp_failed" // compensation also failed (needs manual fix)
)

// Step defines one step in a saga with its forward and compensating actions.
type Step struct {
	Name       string
	Action     func(ctx context.Context, state map[string]any) error
	Compensate func(ctx context.Context, state map[string]any) error
}

// StepResult records the outcome of a single step execution.
type StepResult struct {
	Name          string
	Status        StepStatus
	Error         string
	ExecutedAt    time.Time
	CompensatedAt *time.Time
}

// SagaExecution tracks the full execution of a saga instance.
type SagaExecution struct {
	ID        string
	Name      string
	Status    SagaStatus
	Steps     []StepResult
	State     map[string]any // shared state passed between steps
	StartedAt time.Time
	EndedAt   *time.Time
}

// Orchestrator executes a saga, tracking state and running compensations on failure.
type Orchestrator struct {
	log *zap.Logger
}

func NewOrchestrator(log *zap.Logger) *Orchestrator {
	return &Orchestrator{log: log}
}

// Execute runs the saga steps in order.
// On failure at step N, compensates steps N-1 down to 0 in reverse order.
// Returns the execution record for observability.
func (o *Orchestrator) Execute(ctx context.Context, sagaName string, steps []Step, initialState map[string]any) *SagaExecution {
	exec := &SagaExecution{
		ID:        fmt.Sprintf("%s-%d", sagaName, time.Now().UnixNano()),
		Name:      sagaName,
		Status:    SagaRunning,
		Steps:     make([]StepResult, len(steps)),
		State:     initialState,
		StartedAt: time.Now(),
	}
	if exec.State == nil {
		exec.State = make(map[string]any)
	}

	// Initialise step results
	for i, s := range steps {
		exec.Steps[i] = StepResult{Name: s.Name, Status: StepPending}
	}

	o.log.Info("saga started", zap.String("saga", sagaName), zap.String("id", exec.ID))

	// Forward pass: execute steps in order
	failedAt := -1
	for i, step := range steps {
		o.log.Debug("saga step executing",
			zap.String("saga", sagaName), zap.String("step", step.Name))

		err := step.Action(ctx, exec.State)
		exec.Steps[i].ExecutedAt = time.Now()

		if err != nil {
			exec.Steps[i].Status = StepFailed
			exec.Steps[i].Error = err.Error()
			failedAt = i
			o.log.Error("saga step failed",
				zap.String("saga", sagaName),
				zap.String("step", step.Name),
				zap.Error(err))
			break
		}
		exec.Steps[i].Status = StepCompleted
		o.log.Debug("saga step completed",
			zap.String("saga", sagaName), zap.String("step", step.Name))
	}

	if failedAt == -1 {
		// All steps succeeded
		now := time.Now()
		exec.Status = SagaCompleted
		exec.EndedAt = &now
		o.log.Info("saga completed", zap.String("saga", sagaName), zap.String("id", exec.ID))
		return exec
	}

	// Backward pass: compensate completed steps in reverse order
	o.log.Warn("saga compensating",
		zap.String("saga", sagaName),
		zap.Int("failed_at_step", failedAt))

	compFailed := false
	for i := failedAt - 1; i >= 0; i-- {
		step := steps[i]
		if step.Compensate == nil {
			o.log.Warn("no compensation defined for step",
				zap.String("step", step.Name))
			continue
		}

		o.log.Debug("compensating step",
			zap.String("saga", sagaName), zap.String("step", step.Name))

		if err := step.Compensate(ctx, exec.State); err != nil {
			compFailed = true
			o.log.Error("compensation failed — manual intervention required",
				zap.String("saga", sagaName),
				zap.String("step", step.Name),
				zap.Error(err))
			exec.Steps[i].Error += fmt.Sprintf(" | compensation failed: %v", err)
		} else {
			now := time.Now()
			exec.Steps[i].Status = StepCompensated
			exec.Steps[i].CompensatedAt = &now
		}
	}

	now := time.Now()
	exec.EndedAt = &now
	if compFailed {
		exec.Status = SagaCompFailed // needs manual fix
	} else {
		exec.Status = SagaFailed // cleanly rolled back
	}

	o.log.Warn("saga rolled back",
		zap.String("saga", sagaName),
		zap.String("status", string(exec.Status)))
	return exec
}

// ─── Order Placement Saga ─────────────────────────────────────────────────────

// OrderSagaState holds shared state passed between saga steps.
type OrderSagaState struct {
	UserID     string
	OrderID    string
	Items      []OrderItemSpec
	TotalPrice float64
	PaymentID  string
}

// BuildOrderPlacementSaga returns the steps for the order placement saga.
// Each step has a forward action and a compensating action.
//
// The actual implementations are injected as closures so the saga definition
// doesn't import repository/service packages (dependency inversion).
func BuildOrderPlacementSaga(
	reserveStock func(ctx context.Context, state map[string]any) error,
	releaseStock func(ctx context.Context, state map[string]any) error,
	createOrder func(ctx context.Context, state map[string]any) error,
	cancelOrder func(ctx context.Context, state map[string]any) error,
	chargePayment func(ctx context.Context, state map[string]any) error,
	refundPayment func(ctx context.Context, state map[string]any) error,
	confirmOrder func(ctx context.Context, state map[string]any) error,
) []Step {
	return []Step{
		{
			Name:       "ReserveStock",
			Action:     reserveStock,
			Compensate: releaseStock,
		},
		{
			Name:       "CreateOrder",
			Action:     createOrder,
			Compensate: cancelOrder,
		},
		{
			Name:       "ChargePayment",
			Action:     chargePayment,
			Compensate: refundPayment,
		},
		{
			Name:       "ConfirmOrder",
			Action:     confirmOrder,
			Compensate: cancelOrder, // cancel if confirmation fails
		},
	}
}
