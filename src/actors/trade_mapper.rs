pub struct TradeMapper;
pub struct PositionMapper;

pub struct NetWorkMapper;
// use super::http_data::TradeRe;
use crate::actors::database::get_connect;
// use log::info;
use mysql::*;
use mysql::prelude::*;
use serde_json::Value;
// use super::db_data::Trade;


impl TradeMapper {


  // 插入bybit数据
  pub fn insert_bybit_trade(trades:Vec<Value>, name: &str) -> bool {
    // 连接数据库
    let mut conn = get_connect();
    // let query_id = conn.exec_first(, params)
    let mut value = "";


    if name == "mmteam1" {
      value =r"INSERT IGNORE INTO mmteam1_traders (tra_order_id, th_id, time, symbol, side, price, qty, quote_qty, commission, type)
      VALUES (:tra_order_id, :th_id, :time, :symbol, :side, :price, :qty, :quote_qty, :commission, :type)";
    } else if name == "hmaker05" {
      value =r"INSERT IGNORE INTO hmaker05_traders (tra_order_id, th_id, time, symbol, side, price, qty, quote_qty, commission, type)
      VALUES (:tra_order_id, :th_id, :time, :symbol, :side, :price, :qty, :quote_qty, :commission, :type)";
    } else {
      value =r"INSERT IGNORE INTO bybit_trader_histories (tra_order_id, th_id, time, symbol, side, price, qty, quote_qty, commission, type)
      VALUES (:tra_order_id, :th_id, :time, :symbol, :side, :price, :qty, :quote_qty, :commission, :type)";
    }



    let flag = conn.exec_batch(
      value,
      trades.iter().map(|p| params! {
        "th_id" => &p["th_id"],
        "tra_order_id" => &p["tra_order_id"],
        "time" => &p["time"],
        "symbol" => &p["symbol"],
        "side" => &p["side"],
        "price" => &p["price"],
        "qty" => &p["qty"],
        "quote_qty" => &p["quote_qty"],
        "commission" => &p["commission"],
        "type" => &p["type"]
      })
    );

  // let um1 = conn.query_map(
  // "select * from trate_histories",
  // |(th_id, tra_symbol, tra_order_id, tra_commision, tra_time, is_maker, position_side, price, qty, quote_qty, realized_pnl, side)| {
  //     Trade{th_id, tra_symbol, tra_order_id, tra_commision, tra_time, is_maker, position_side, price, qty, quote_qty, realized_pnl, side}
  // }
  // ).unwrap();

  // println!("查询到的数据{:?}", um1);

    match flag {
      Ok(_c) => {
        println!("insert success!");
        return true;
      },
      Err(e) => {
        eprintln!("error:{}", e);
        return false;
      }
    }
  }



}




