use rolling_ohlc::RollingOhlc;
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

#[derive(Serialize)]
struct OhlcData {
    symbol: String,
    timestamp: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

fn read_price_data_from_file(file_path: &str) -> Result<Vec<PriceData>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut prices = Vec::new();

    for line in reader.lines() {
        let json_str = line?;
        let curr: PriceData = serde_json::from_str(&json_str)?;
        prices.push(curr);
    }

    Ok(prices)
}

fn process_price_data(prices: Vec<PriceData>, window: u64) -> Vec<OhlcData> {
    let mut ohlcs = std::collections::HashMap::new();
    let mut ohlc_values = Vec::new();

    for price in prices {
        let ohlc = ohlcs.entry(price.s.clone()).or_insert_with(|| RollingOhlc::new(window));
        if let Some(ohlc) = ohlc.update(price.T, price.b.parse::<f64>().unwrap_or(0.0)) {
            ohlc_values.push(OhlcData {
                symbol: price.s,
                timestamp: price.T,
                open: ohlc.open,
                high: ohlc.high,
                low: ohlc.low,
                close: ohlc.close,
            });
        }
    }

    ohlc_values
}

fn write_ohlc_data_to_file(ohlc_values: Vec<OhlcData>, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(file_path)?;
    for ohlc in ohlc_values {
        let json_str = serde_json::to_string(&ohlc)?;
        file.write_all(json_str.as_bytes())?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn main() {
    let window: u64 = 300;
    let input_file_path = "../data/dataset-a.txt";
    let output_file_path = "../data/output.txt";

    match read_price_data_from_file(input_file_path) {
        Ok(prices) => {
            let ohlc_values = process_price_data(prices, window);
            if let Err(err) = write_ohlc_data_to_file(ohlc_values, output_file_path) {
                eprintln!("Failed to write OHLC data to file: {}", err);
            }
        }
        Err(err) => {
            eprintln!("Failed to read price data from file: {}", err);
        }
    }
}
