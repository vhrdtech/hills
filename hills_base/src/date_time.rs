use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct UtcDateTime {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    secs: u32,
    milli: u32,
}

impl From<DateTime<Utc>> for UtcDateTime {
    fn from(dt: DateTime<Utc>) -> Self {
        let date = dt.date_naive();
        let time = dt.time();
        Self {
            year: date.year(),
            month: date.month(),
            day: date.day(),
            hour: time.hour(),
            min: time.minute(),
            secs: time.second(),
            milli: time.nanosecond() / 1000,
        }
    }
}

impl From<UtcDateTime> for DateTime<Utc> {
    fn from(dt: UtcDateTime) -> Self {
        let date = NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day);
        let time = NaiveTime::from_hms_milli_opt(dt.hour, dt.min, dt.secs, dt.milli);
        if let (Some(date), Some(time)) = (date, time) {
            DateTime::from_naive_utc_and_offset(NaiveDateTime::new(date, time), Utc)
        } else {
            DateTime::default()
        }
    }
}
