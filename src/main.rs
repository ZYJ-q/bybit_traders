use std::collections::VecDeque;
use std::{collections::HashMap, fs, time::Duration};

use chrono::{DateTime, NaiveDateTime, Utc, Local};
use log::{debug, info, warn};
use serde_json::{Map, Value};
// use tokio::{sync::broadcast::{self, Receiver}};
use bybit_trader::adapters::bybit::futures::http::actions::ByBitFuturesApi;
use bybit_trader::base::ssh::SshClient;
use bybit_trader::base::wxbot::WxbotHttpClient;
use bybit_trader::actors::*;
// use test_alarm::models::http_data::*;

#[warn(unused_mut, unused_variables, dead_code)]
async fn real_time(
    // binance_futures_api: BinanceFuturesApi,
    symbols: &Vec<Value>,
    mut ssh_api: SshClient,
    wx_robot: WxbotHttpClient,
    ori_fund: f64,
) {
    //rece: &mut Receiver<&str>){
    info!("get ready for real time loop");
    let mut running = false;
    let mut minut_end = 1;
    let mut time_minut_id = 2;

    // 每个品种的上一个trade_id
    let mut last_trade_ids: HashMap<String, u64> = HashMap::new();
    for symbol_v in symbols {
        let symbol = String::from(symbol_v.as_str().unwrap());
        let symbol = format!("{}", symbol);
        last_trade_ids.insert(symbol, 0);
    }

    // 交易历史
    

    // let mut total_trade: VecDeque<Value> = VecDeque::new();

    // 净值数据
    
    

    info!("begin real time loop");
    // 监控循环
    loop {
        info!("again");
        // json对象
        let mut response: Map<String, Value> = Map::new();
        let mut json_data: Map<String, Value> = Map::new();
        let mut map: Map<String, Value> = Map::new();
        map.insert(String::from("productId"), Value::from("TRADER_001"));
        let now = Utc::now();
        let date = format!("{}", now.format("%Y/%m/%d %H:%M:%S"));
        

        

        
        // 监控服务器状态
        info!("server process");

        // 成交历史(更新所有)
        info!("trade history");

        let bybit = trade_mapper::TradeMapper::get_positions().unwrap();



        for f_config in bybit {
            let mut trade_bybit_histories: VecDeque<Value> = VecDeque::new();

            if &f_config.tra_venue == "ByBit" {
                let bybit_futures_api=ByBitFuturesApi::new(
                    "https://api.bybit.com",
                        &f_config.api_key,
                        &f_config.secret_key,
                );
                let name = f_config.tra_id;
                let category = "linear";
                    if let Some(data) = bybit_futures_api.get_order_history(category, &minut_end, &time_minut_id).await {
                        let v: Value = serde_json::from_str(&data).unwrap();
                        let result = v.as_object().unwrap().get("result").unwrap().as_object().unwrap();
                        let category = result.get("category").unwrap().as_str().unwrap();
                        let list = result.get("list").unwrap().as_array().unwrap();
                        let mut trade_bybit_object: Map<String, Value> = Map::new();
                        
                        for i in list{
                            let obj = i.as_object().unwrap();
                            
                            let time:u64 = obj.get("createdTime").unwrap().as_str().unwrap().parse().unwrap();
                            let symbol = obj.get("symbol").unwrap().as_str().unwrap();
                            let th_id = obj.get("orderLinkId").unwrap().as_str().unwrap();
                            let tra_order_id = obj.get("orderId").unwrap().as_str().unwrap();
                            let side = obj.get("side").unwrap().as_str().unwrap();
                            let price = obj.get("avgPrice").unwrap().as_str().unwrap();
                            let qty = obj.get("cumExecQty").unwrap().as_str().unwrap();
                            let commission = obj.get("cumExecFee").unwrap().as_str().unwrap();
                            let quote_qty = obj.get("cumExecValue").unwrap().as_str().unwrap();
    
                            trade_bybit_object.insert(String::from("tra_order_id"), Value::from(tra_order_id));
                            trade_bybit_object.insert(String::from("th_id"), Value::from(th_id));
                            trade_bybit_object.insert(String::from("time"), Value::from(time));
                            trade_bybit_object.insert(String::from("symbol"), Value::from(symbol));
                            trade_bybit_object.insert(String::from("side"), Value::from(side));
                            trade_bybit_object.insert(String::from("price"), Value::from(price));
                            trade_bybit_object.insert(String::from("qty"), Value::from(qty));
                            trade_bybit_object.insert(String::from("quote_qty"), Value::from(quote_qty));
                            trade_bybit_object.insert(String::from("commission"), Value::from(commission));
                            trade_bybit_object.insert(String::from("type"), Value::from(category));
                            trade_bybit_object.insert(String::from("name"), Value::from(name));
    
                        }
                        trade_bybit_histories.push_back(Value::from(trade_bybit_object));
                        println!("历史数据{:?}, 名字{}", Vec::from(trade_bybit_histories.clone()), name);
                    }

                    let category_spot = "spot";


                    if let Some(data) = bybit_futures_api.get_order_history(category_spot, &minut_end, &time_minut_id).await {
                        let v: Value = serde_json::from_str(&data).unwrap();
                        let result = v.as_object().unwrap().get("result").unwrap().as_object().unwrap();
                        let category = result.get("category").unwrap().as_str().unwrap();
                        let list = result.get("list").unwrap().as_array().unwrap();
                        let mut trade_bybit_object: Map<String, Value> = Map::new();
                        
                        for i in list{
                            let obj = i.as_object().unwrap();
                            
                            let time:u64 = obj.get("createdTime").unwrap().as_str().unwrap().parse().unwrap();
                            let symbol = obj.get("symbol").unwrap().as_str().unwrap();
                            let th_id = obj.get("orderLinkId").unwrap().as_str().unwrap();
                            let tra_order_id = obj.get("orderId").unwrap().as_str().unwrap();
                            let side = obj.get("side").unwrap().as_str().unwrap();
                            let price = obj.get("avgPrice").unwrap().as_str().unwrap();
                            let qty = obj.get("cumExecQty").unwrap().as_str().unwrap();
                            let commission = obj.get("cumExecFee").unwrap().as_str().unwrap();
                            let quote_qty = obj.get("cumExecValue").unwrap().as_str().unwrap();
    
                            trade_bybit_object.insert(String::from("tra_order_id"), Value::from(tra_order_id));
                            trade_bybit_object.insert(String::from("th_id"), Value::from(th_id));
                            trade_bybit_object.insert(String::from("time"), Value::from(time));
                            trade_bybit_object.insert(String::from("symbol"), Value::from(symbol));
                            trade_bybit_object.insert(String::from("side"), Value::from(side));
                            trade_bybit_object.insert(String::from("price"), Value::from(price));
                            trade_bybit_object.insert(String::from("qty"), Value::from(qty));
                            trade_bybit_object.insert(String::from("quote_qty"), Value::from(quote_qty));
                            trade_bybit_object.insert(String::from("commission"), Value::from(commission));
                            trade_bybit_object.insert(String::from("type"), Value::from(category));
                            trade_bybit_object.insert(String::from("name"), Value::from(name));
    
                        }
                        trade_bybit_histories.push_back(Value::from(trade_bybit_object));
                        println!("历史数据{:?}, 名字spot{}", Vec::from(trade_bybit_histories.clone()), name);
                    }
            }
    
            let res = trade_mapper::TradeMapper::insert_bybit_trade(Vec::from(trade_bybit_histories.clone()));
            println!("插入历史交易数据是否成功{}", res);
    
             
        }


        let time_min = Local::now().timestamp_millis();
        let last_time_min = time_min - 1000*60*60*24 * minut_end;
        if time_minut_id == 720 {
            time_minut_id = 2;
            if minut_end != 0 {
                minut_end -= 2
            } else {
                minut_end = 0
            }
        } else {
            if last_time_min < time_min {
                time_minut_id += 2
            } else if last_time_min == time_min  {
                time_minut_id = time_minut_id
            } else {
                time_minut_id -= 2
            }
        }

    

            

        
        



        

        // 输出日志
        // debug!("writing {}", json_file);


        

        // let net_worth_res = trade_mapper::NetWorkMapper::insert_net_worth(Vec::from(net_worth_histories.clone()));
        // print!("输出的净值数据信息{}", net_worth_res);

        // 等待下次执行
        info!("waiting for next real time task...({})", 1000 * 10);
        tokio::time::delay_for(Duration::from_millis(1000 * 10)).await;
    }
}

#[warn(unused_mut, unused_variables)]
#[tokio::main]
async fn main() {
    // 日志
    log4rs::init_file("./log4rs.yaml", Default::default()).unwrap();

    init();

    // 测试用api
    // let api_key="JwYo1CffkOLqmv2sC3Qhe2Qu5GgzbeLVw2BxWB5HgK6tnmc8yGfkzLuDImBgDkXm";
    // let api_secret="7FtQARZqM2PDgIZ5plr3nwEVYBXXbvmSuvmpf6Viz9e7Cq2B87grRTG3VZQiEC5C";

    // 连接数据库
    // let config_db: Value =
    //     serde_json::from_str(&fs::read_to_string("./configs/database.json").unwrap()).unwrap();

    // 读取配置
    let config: Value = serde_json::from_str(
        &fs::read_to_string("./configs/total.json").expect("Unable to read file"),
    )
    .expect("Unable to parse");

    // 任务间通信信道
    // let (send, mut rece) = broadcast::channel(32);

    // 创建任务
    let real_time_handle = tokio::spawn(async move {
        // let mut futures_config: Map<String, Value> = Map::new();
        
        // let mut servers_config = Map::new();
        let binance_config = config.get("Binance").unwrap();
        let bybit_config = config.get("ByBit").unwrap();
        // let binance_future_config = binance_config.get("futures").unwrap();
        let binance_future_config = binance_config.get("futures").unwrap().as_array().unwrap();
        let bybit_futures_config = bybit_config.get("futures").unwrap().as_array().unwrap();
        let server_config = config.get("Server").unwrap();
        let symbols = config.get("Symbols").unwrap().as_array().unwrap();
        let key = config.get("Alarm").unwrap().get("webhook").unwrap().as_str().unwrap();
        // info!("获取key");
        let mut wxbot = String::from("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=");
        wxbot.push_str(key);
        info!("wxbot  {}", wxbot);
        let wx_robot = WxbotHttpClient::new(&wxbot);
        info!("preparing...");

        // for s_config in server_config{
        //     let obj = s_config.as_object().unwrap(); 
        //     let host = obj.get("host").unwrap().as_str().unwrap();
        //     let port = obj.get("port").unwrap().as_str().unwrap();
        //     let username = obj.get("username").unwrap().as_str().unwrap();
        //     let password = obj.get("password").unwrap().as_str().unwrap();
        //     let root_path = obj.get("root_path").unwrap().as_str().unwrap();
        //     let root_name = obj.get("root_name").unwrap().as_str().unwrap();
        //     servers_config.insert(String::from("host"), Value::from(host));
        //     servers_config.insert(String::from("port"), Value::from(port));
        //     servers_config.insert(String::from("username"), Value::from(username));
        //     servers_config.insert(String::from("password"), Value::from(password));
        //     servers_config.insert(String::from("root_path"), Value::from(root_path));
        //     servers_config.insert(String::from("root_name"), Value::from(root_name));
        // }
        
        
        
        let ssh_api = SshClient::new(
            server_config.get("host").unwrap().as_str().unwrap(),
            server_config.get("port").unwrap().as_str().unwrap(),
            server_config.get("username").unwrap().as_str().unwrap(),
            server_config.get("password").unwrap().as_str().unwrap(),
            server_config.get("root_path").unwrap().as_str().unwrap(),
            server_config.get("root_name").unwrap().as_str().unwrap(),
        );
        

        
        // for f_config in binance_future_config{
        //     let obj = f_config.as_object().unwrap(); 
        //     let base_url = obj.get("base_url").unwrap().as_str().unwrap();
        //     let api_key = obj.get("api_key").unwrap().as_str().unwrap();
        //     let secret_key = obj.get("secret_key").unwrap().as_str().unwrap();
        //     futures_config.insert(String::from("base_url"), Value::from(base_url));
        //     futures_config.insert(String::from("api_key"), Value::from(api_key));
        //     futures_config.insert(String::from("secret_key"), Value::from(secret_key));
        // }

        info!("created ssh client");
        // let binance_futures_api=BinanceFuturesApi::new(
        //     binance_config
        //         .get("futures")
        //         .unwrap()
        //         .get("base_url")
        //         .unwrap()
        //         .as_str()
        //         .unwrap(),
        //     binance_config
        //         .get("futures")
        //         .unwrap()
        //         .get("api_key")
        //         .unwrap()
        //         .as_str()
        //         .unwrap(),
        //     binance_config
        //         .get("futures")
        //         .unwrap()
        //         .get("secret_key")
        //         .unwrap()
        //         .as_str()
        //         .unwrap(),
        // );

        
        info!("created http client");
        real_time(symbols, ssh_api, wx_robot, 500.0).await;
    });

    // 开始任务
    info!("alarm begin(binance account)");
    real_time_handle.await.unwrap();
    info!("alarm done");
}
