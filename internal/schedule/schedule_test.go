package schedule

import (
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
)

func jobWithGrace(minutes int) config.Job {
	enabled := true
	return config.Job{
		ID: "test", Enabled: &enabled,
		Schedule: config.Schedule{Kind: "cron", Expression: "0 9 * * *", Timezone: "Asia/Hong_Kong", CatchUpGraceMinutes: &minutes},
	}
}

func instant(t *testing.T, value string) time.Time {
	t.Helper()
	got, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestZeroGraceStillAllowsNormalPollSkew(t *testing.T) {
	job := jobWithGrace(0)
	after := instant(t, "2026-08-22T08:59:00+08:00")
	now := instant(t, "2026-08-22T09:00:30+08:00")
	got, err := Between(job, after, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Outcome != "due" || got[0].Trigger != "cron" {
		t.Fatalf("got %+v, want one on-time occurrence", got)
	}
}

func TestZeroGraceMarksDowntimeOccurrenceMissed(t *testing.T) {
	job := jobWithGrace(0)
	after := instant(t, "2026-08-22T08:59:00+08:00")
	now := instant(t, "2026-08-22T09:02:00+08:00")
	got, err := Between(job, after, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Outcome != "missed" {
		t.Fatalf("got %+v, want one missed occurrence", got)
	}
}

func TestCatchUpAndTimezone(t *testing.T) {
	job := jobWithGrace(120)
	after := instant(t, "2026-08-22T00:00:00Z")
	now := instant(t, "2026-08-22T02:00:00Z")
	got, err := Between(job, after, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Trigger != "catchup" || got[0].ScheduledFor != instant(t, "2026-08-22T01:00:00Z") {
		t.Fatalf("got %+v", got)
	}
}

func TestRepeatedDSTMinuteProducesOneOccurrence(t *testing.T) {
	minutes := 180
	job := jobWithGrace(minutes)
	job.Schedule = config.Schedule{Kind: "cron", Expression: "30 1 * * *", Timezone: "America/New_York", CatchUpGraceMinutes: &minutes}
	got, err := Between(job, instant(t, "2026-11-01T00:00:00-04:00"), instant(t, "2026-11-01T03:00:00-05:00"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("repeated wall-clock minute produced %d occurrences: %+v", len(got), got)
	}
}

func TestOnceRunsAtMostOnceAcrossCursor(t *testing.T) {
	minutes := 30
	job := jobWithGrace(minutes)
	job.Schedule = config.Schedule{Kind: "once", At: "2026-08-22T09:00:00+08:00", Timezone: "Asia/Hong_Kong", CatchUpGraceMinutes: &minutes}
	now := instant(t, "2026-08-22T09:00:30+08:00")
	first, err := Between(job, instant(t, "2026-08-22T08:00:00+08:00"), now)
	if err != nil || len(first) != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := Between(job, now, now.Add(time.Hour))
	if err != nil || len(second) != 0 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
}

func TestEventScheduleNeverProducesClockOccurrences(t *testing.T) {
	minutes := 120
	job := jobWithGrace(minutes)
	job.Schedule = config.Schedule{Kind: config.ScheduleEvent, Timezone: "Asia/Hong_Kong", CatchUpGraceMinutes: &minutes}
	got, err := Between(job, instant(t, "2020-01-01T00:00:00Z"), instant(t, "2030-01-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("event schedule produced clock occurrences: %+v", got)
	}
}
