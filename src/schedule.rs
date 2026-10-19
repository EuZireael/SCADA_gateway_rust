//! Расписание опроса тегов контроллера. Контроллер опрашивается циклом с периодом `cycle_period_ms()` (минимальный
//! `pollingRate` его тегов); тег с настройкой «период» (`tag_settings.rs`) читается реже — каждый N-й цикл.
//!
//! Срок хранится как момент следующего чтения и сдвигается на период, а не пересчитывается от последнего чтения:
//! ритм не уходит от дрожания тикера. Тик считается подходящим с запасом в полцикла, иначе чтение, пришедшееся на
//! 2,999 с при сроке 3,000 с, ушло бы на цикл позже, и период 3 с превратился бы в 4 с. Чаще цикла тег не читается.

use std::time::{Duration, Instant};

use crate::model::Tag;

/// Сроки следующего чтения по слотам тегов контроллера (`Tag::slot`).
pub struct PollSchedule {
    /// `None` — расписания у тега нет (читается каждый цикл) или он ещё не читался.
    next: Vec<Option<Instant>>,
    slack: Duration,
}

impl PollSchedule {
    /// Расписание для `slots` тегов контроллера с циклом `cycle`.
    pub fn new(slots: usize, cycle: Duration) -> Self {
        PollSchedule { next: vec![None; slots], slack: cycle / 2 }
    }

    /// Пора ли читать тег в этом цикле. Если да, следующий срок сдвигается; зовётся один раз за цикл на тег.
    /// Тег без настройки «период» читается всегда; первое чтение — в первом же цикле.
    pub fn take_due(&mut self, tag: &Tag, now: Instant) -> bool {
        let Some(slot) = self.next.get_mut(tag.slot) else { return true };
        let Some(period) = tag.tuning.poll_period() else {
            *slot = None;
            return true;
        };
        match *slot {
            Some(due) if now + self.slack < due => false,
            Some(due) => {
                let next = due + period;
                // Не догоняем пропущенное (пауза, переподключение): следующий срок — через период от сейчас.
                *slot = Some(if next > now { next } else { now + period });
                true
            }
            None => {
                *slot = Some(now + period);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Tag;
    use crate::tag_settings::TagSettings;

    fn tag(slot: usize, period_ms: Option<u64>) -> Tag {
        let cfg: crate::config::TagConfig = serde_yaml_ng::from_str(
            r#"{name: "A.B.V1.ST", nodeId: "ns=2;s=1", dataType: FLOAT, pollingRate: 1000, enabled: true}"#,
        )
        .expect("тег");
        let t = Tag { slot, ..Tag::from_config(&cfg) };
        if let Some(ms) = period_ms {
            t.tuning.apply(Some(&TagSettings { deadband: 0.0, poll_period_ms: Some(ms) }));
        }
        t
    }

    /// Какие из тиков (с шагом цикла и дрожанием) тег читается.
    fn reads(t: &Tag, cycle_ms: u64, jitter_ms: i64, ticks: u64) -> Vec<u64> {
        let mut sched = PollSchedule::new(8, Duration::from_millis(cycle_ms));
        let start = Instant::now();
        (0..ticks)
            .filter(|n| {
                let at = start + Duration::from_millis(cycle_ms * n)
                    - Duration::from_millis(if n % 2 == 0 { 0 } else { jitter_ms as u64 });
                sched.take_due(t, at)
            })
            .collect()
    }

    #[test]
    fn a_tag_without_settings_is_read_every_cycle() {
        assert_eq!(reads(&tag(0, None), 1000, 0, 5), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn period_three_seconds_at_one_second_cycle_is_every_third_cycle() {
        assert_eq!(reads(&tag(0, Some(3000)), 1000, 0, 10), vec![0, 3, 6, 9]);
    }

    #[test]
    fn timer_jitter_does_not_stretch_the_period() {
        // Тики приходят на 20 мс раньше срока в нечётные циклы: 3 с остаются 3 с, а не 4 с.
        assert_eq!(reads(&tag(0, Some(3000)), 1000, 20, 10), vec![0, 3, 6, 9]);
    }

    #[test]
    fn a_period_shorter_than_the_cycle_never_speeds_the_cycle_up() {
        assert_eq!(reads(&tag(0, Some(300)), 1000, 0, 4), vec![0, 1, 2, 3]);
    }

    #[test]
    fn a_missed_stretch_is_not_caught_up() {
        let t = tag(0, Some(3000));
        let mut sched = PollSchedule::new(1, Duration::from_millis(1000));
        let start = Instant::now();
        assert!(sched.take_due(&t, start));
        // Контроллер был недоступен 20 с: одно чтение, а не шесть подряд.
        let later = start + Duration::from_secs(20);
        assert!(sched.take_due(&t, later));
        assert!(!sched.take_due(&t, later + Duration::from_secs(1)));
        assert!(!sched.take_due(&t, later + Duration::from_secs(2)));
        assert!(sched.take_due(&t, later + Duration::from_secs(3)));
    }

    #[test]
    fn removing_the_setting_returns_the_tag_to_every_cycle() {
        let t = tag(0, Some(3000));
        let mut sched = PollSchedule::new(1, Duration::from_millis(1000));
        let start = Instant::now();
        assert!(sched.take_due(&t, start));
        assert!(!sched.take_due(&t, start + Duration::from_secs(1)));
        t.tuning.apply(None);
        assert!(sched.take_due(&t, start + Duration::from_secs(2)));
        assert!(sched.take_due(&t, start + Duration::from_secs(3)));
    }
}
