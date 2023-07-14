use std::collections::VecDeque;

#[derive(Debug)]
pub struct Ohlc {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

pub struct RollingOhlc {
    window: u64,
    prices: VecDeque<(u64, f64)>,
}

impl RollingOhlc {
    pub fn new(window: u64) -> Self {
        RollingOhlc {
            window,
            prices: VecDeque::new(),
        }
    }

    pub fn update(&mut self, timestamp: u64, price: f64) -> Option<Ohlc> {
        self.prices.push_back((timestamp, price));

        while let Some((ts, _)) = self.prices.front() {
            if timestamp - ts > self.window {
                self.prices.pop_front();
            } else {
                break;
            }
        }

        if let Some((_, open)) = self.prices.front() {
            let close = price;
            let high = self.prices.iter().map(|(_, p)| *p).fold(f64::NEG_INFINITY, f64::max);
            let low = self.prices.iter().map(|(_, p)| *p).fold(f64::INFINITY, f64::min);
            Some(Ohlc { open: *open, high, low, close })
        } else {
            None
        }
    }
}
