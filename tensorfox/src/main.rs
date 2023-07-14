use rolling_ohlc::{RollingOhlc};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

#[derive(Deserialize)]
struct PriceData {
    e: String,
    u: u64,
    s: String,
    b: String,
    B: String,
    a: String,
    A: String,
    T: u64,
    E: u64,
}
struct PriceDataPased {
    symbol : String, 
    timestamp : u64,
    price : f64,
}

#[derive(Serialize)]
pub struct OhlcData {
    pub symbol: String, 
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}


fn main() {
    let window: u64 = 300;
    let file = File::open("../data/dataset-a.txt").expect("Failed to open file");
    let reader = BufReader::new(file);
    let mut prices : Vec<PriceDataPased> = Vec::new();
    for line in reader.lines() {
        if let Ok(json_str) = line {
            let curr : PriceData = serde_json::from_str(&json_str).expect("Failed to deserialize JSON");
            let curr_ohcl = PriceDataPased {
                symbol: curr.s,
                timestamp: curr.T,
                price: curr.b.parse::<f64>().unwrap(),
            };
            prices.push(curr_ohcl);
        } else {
            println!("Failed to read line");
            break;
        }
    }
    let mut ohlcs = std::collections::HashMap::new();
    let mut ohlc_values: Vec<OhlcData> = Vec::new();
    for price in prices {
        let ohlc = ohlcs
            .entry(price.symbol.clone())
            .or_insert_with(|| RollingOhlc::new(window));
        if let Some(ohlc) = ohlc.update(price.timestamp, price.price) {
            ohlc_values.push(OhlcData {
                symbol: price.symbol,
                timestamp: price.timestamp,
                open: ohlc.open,
                high: ohlc.high,
                low: ohlc.low,
                close: ohlc.close,
            });
        }
    }
    let mut out = File::create("../data/output.txt").expect("Failed to create file");
    for ohls in ohlc_values {
        let json_str = serde_json::to_string(&ohls).expect("Failed to serialize vector to JSON");
        out.write_all(json_str.as_bytes()).expect("Failed to write to file");
        out.write("\n".as_bytes()).expect("Failed to write to file");
    }
}
