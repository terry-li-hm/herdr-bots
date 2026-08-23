package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/terry-li-hm/herdr-bots/internal/config"
)

const NormalPollTolerance = 60 * time.Second

var parser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type Occurrence struct {
	Key          string
	ScheduledFor time.Time
	Trigger      string
	Outcome      string
	Detail       string
}

func Between(job config.Job, after, now time.Time) ([]Occurrence, error) {
	loc, err := time.LoadLocation(job.Schedule.Timezone)
	if err != nil {
		return nil, err
	}
	var moments []time.Time
	switch job.Schedule.Kind {
	case config.ScheduleCron:
		sched, err := parser.Parse(job.Schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("parse cron: %w", err)
		}
		cursor := after.In(loc)
		seenWallClock := map[string]bool{}
		for next := sched.Next(cursor); !next.After(now.In(loc)); next = sched.Next(next) {
			// A repeated wall-clock minute during a DST fall-back is one occurrence,
			// not two different authorities to run the same job.
			wallClock := next.In(loc).Format("2006-01-02T15:04")
			if !seenWallClock[wallClock] {
				moments = append(moments, next)
				seenWallClock[wallClock] = true
			}
			if len(moments) >= 1000 {
				return nil, fmt.Errorf("more than 1000 occurrences are due")
			}
		}
	case config.ScheduleOnce:
		at, err := time.Parse(time.RFC3339, job.Schedule.At)
		if err != nil {
			return nil, err
		}
		if at.After(after) && !at.After(now) {
			moments = append(moments, at)
		}
	case config.ScheduleEvent:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported schedule kind %q", job.Schedule.Kind)
	}

	out := make([]Occurrence, 0, len(moments))
	for _, moment := range moments {
		late := now.Sub(moment)
		occ := Occurrence{Key: occurrenceKey(job.Schedule.Kind, moment, loc), ScheduledFor: moment.UTC(), Trigger: "cron", Outcome: "due"}
		if job.Schedule.Kind == config.ScheduleOnce {
			occ.Trigger = "once"
		}
		if late <= NormalPollTolerance {
			out = append(out, occ)
			continue
		}
		if late <= job.CatchUpGrace() {
			occ.Trigger = "catchup"
			occ.Detail = fmt.Sprintf("started %s late", late.Round(time.Second))
			out = append(out, occ)
			continue
		}
		occ.Outcome = "missed"
		occ.Detail = fmt.Sprintf("due %s ago, beyond %s catch-up grace", late.Round(time.Second), job.CatchUpGrace())
		out = append(out, occ)
	}
	return out, nil
}

func occurrenceKey(kind string, moment time.Time, location *time.Location) string {
	return kind + ":" + moment.In(location).Format("2006-01-02T15:04")
}
