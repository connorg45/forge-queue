package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

var ErrScheduleNotFound = errors.New("schedule not found")

type Store struct {
	db     *pgxpool.Pool
	parser cron.Parser
}

type CreateParams struct {
	TenantID uuid.UUID
	Name     string
	CronExpr string
	Payload  json.RawMessage
	Queue    string
	Handler  string
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db, parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)}
}

func (s *Store) Create(ctx context.Context, p CreateParams) (Schedule, error) {
	p.Name = strings.TrimSpace(p.Name)
	p.CronExpr = strings.TrimSpace(p.CronExpr)
	p.Queue = strings.TrimSpace(p.Queue)
	p.Handler = strings.TrimSpace(p.Handler)
	if p.Name == "" || p.CronExpr == "" || p.Handler == "" {
		return Schedule{}, errors.New("name, cron_expr, and handler are required")
	}
	if p.Queue == "" {
		p.Queue = "default"
	}
	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(p.Payload) {
		return Schedule{}, errors.New("payload must be valid JSON")
	}
	next, err := Next(p.CronExpr, time.Now(), s.parser)
	if err != nil {
		return Schedule{}, err
	}
	if _, err := s.db.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`, p.TenantID, p.TenantID.String()); err != nil {
		return Schedule{}, err
	}
	row := s.db.QueryRow(ctx, `
		INSERT INTO schedules (id, tenant_id, name, cron_expr, payload, queue, handler, enabled, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8)
		RETURNING id, tenant_id, name, cron_expr, payload, queue, handler, next_run_at, enabled
	`, uuid.New(), p.TenantID, p.Name, p.CronExpr, p.Payload, p.Queue, p.Handler, next)
	return scanSchedule(row)
}

func (s *Store) List(ctx context.Context) ([]Schedule, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, tenant_id, COALESCE(name, ''), cron_expr, COALESCE(payload, '{}'::jsonb),
		       COALESCE(queue, 'default'), handler, next_run_at, enabled
		FROM schedules ORDER BY name, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Schedule, 0)
	for rows.Next() {
		schedule, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, schedule)
	}
	return out, rows.Err()
}

func (s *Store) SetEnabled(ctx context.Context, id uuid.UUID, enabled bool) (Schedule, error) {
	row := s.db.QueryRow(ctx, `
		UPDATE schedules SET enabled = $2
		WHERE id = $1
		RETURNING id, tenant_id, COALESCE(name, ''), cron_expr, COALESCE(payload, '{}'::jsonb),
		          COALESCE(queue, 'default'), handler, next_run_at, enabled
	`, id, enabled)
	return scanSchedule(row)
}

func (s *Store) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := s.db.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrScheduleNotFound
	}
	return nil
}

type scanner interface{ Scan(...any) error }

func scanSchedule(row scanner) (Schedule, error) {
	var schedule Schedule
	err := row.Scan(&schedule.ID, &schedule.TenantID, &schedule.Name, &schedule.CronExpr, &schedule.Payload, &schedule.Queue, &schedule.Handler, &schedule.NextRunAt, &schedule.Enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrScheduleNotFound
	}
	return schedule, err
}
