pub struct TradeMapper;
pub struct PositionMapper;

pub struct NetWorkMapper;
// use super::http_data::TradeRe;
use crate::actors::database::get_connect;
// use log::info;
use mysql::*;
use mysql::prelude::*;
use serde_json::Value;
use super::db_data::Positions;


impl TradeMapper {


  pub fn get_positions() -> Result<Vec<Positions>> {
    // 连接数据库
    let mut conn = get_connect();
    let res = conn.query_map(
      r"select * from trader",
      |(tra_id, tra_venue,  tra_currency, api_key, secret_key, r#type, name, alarm, threshold, borrow, amount, wx_hook)| {
        Positions{ tra_id, tra_venue,  tra_currency, api_key, secret_key, r#type, name, alarm, threshold, borrow, amount, wx_hook }
      } 
    ).unwrap();
    return Ok(res);
  }


  // 插入bybit数据
  pub fn insert_bybit_trade(trades:Vec<Value>) -> bool {
    // 连接数据库
    let mut conn = get_connect();
    // let query_id = conn.exec_first(, params)



    let flag = conn.exec_batch(
      r"INSERT IGNORE INTO bybit_traders (tra_order_id, th_id, time, symbol, side, price, qty, quote_qty, commission, type, name)
      VALUES (:tra_order_id, :th_id, :time, :symbol, :side, :price, :qty, :quote_qty, :commission, :type, :name)",
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
        "type" => &p["type"],
        "name" => &p["name"],
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




