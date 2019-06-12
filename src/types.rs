use chrono::{DateTime, Local};

pub struct SubStoreDatum {
    pub time: DateTime<Local>,
    pub id: i32,
    pub sub: i32
}