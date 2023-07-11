use chrono::{DateTime, Utc, NaiveDateTime, Local};
use reqwest::Method;
use serde_json::Value;
use std::collections::HashMap;

use super::client::ByBitHttpClient;

pub struct ByBitFuturesApi {
    client: ByBitHttpClient,
}

impl ByBitFuturesApi {
    pub fn new(
        base_url: &str,
        api_key: &str,
        api_secret: &str,
    ) -> Self {
        let client = ByBitHttpClient::new(base_url, api_key, api_secret);
        Self { client: client }
    }

    pub async fn get_account_overview(&self, account_type: Option<&str>) -> Option<Value> {
        // let my_currency = String::from(currency.unwrap_or("USDT"));

        let mut params: HashMap<String, Value> = HashMap::new();
        params.insert(
            String::from("accountType"),
            Value::from(account_type),
        );

        let response = self
            .client
            .send(Method::GET, "/v5/account/wallet-balance", true,&mut params)
            .await;

        let res_data = self.client.check_response_data(response);

        println!("账户信息11111111111111111111{:?}", res_data);

        match res_data {
            Some(data) => {
                return Some(serde_json::Value::String(data));
            }
            None => {
                return None;
            }
        }
    }

    pub async fn get_order_history(&self, category:&str, end:&i64, time_id:&i64) -> Option<String> {
        // let my_currency = String::from(currency.unwrap_or("USDT"));

        let mut params: HashMap<String, Value> = HashMap::new();


        let now_time = Utc::now().timestamp_millis();
        params.insert(String::from("category"), Value::from(category));
        params.insert(String::from("limit"), Value::from(50));
        params.insert(String::from("orderStatus"), Value::from("Filled"));
        let time = Local::now().timestamp_millis();
        let last_time = time - 1000*60*60*24 * end;

        let mut end_times = 0;
        
        if time_id == &5 {
            end_times = time - 1000*60*60*24 * end;
        } else {
            end_times = last_time + 1000* (time_id -1) * 60
        }

        
        let start_datetime: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from_timestamp_millis(end_times).unwrap(), Utc,);
        let end_datetime: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from_timestamp_millis(&end_times + 1000*1*60).unwrap(), Utc,);
         
        let end_time= format!("{} ", end_datetime.format("%Y-%m-%d %H:%M:%S"));
        let start_time = format!("{} ", start_datetime.format("%Y-%m-%d %H:%M:%S"));
        println!("-------------bybitend------------{:?}", end_time);
        println!("------------bybitstart----------{}", start_time);

        params.insert(String::from("endTime"), Value::from(&end_times + 1000*1*60));
        params.insert(String::from("startTime"), Value::from(end_times));


        let response = self
            .client
            .send(Method::GET, "/v5/order/history", true,&mut params)
            .await;

        let res_data = self.client.check_response_data(response);

        // println!("历史账户交易信息{:?}", res_data);

        match res_data {
            Some(data) => {
                return Some(data);
            }
            None => {
                return None;
            }
        }
    }
}
