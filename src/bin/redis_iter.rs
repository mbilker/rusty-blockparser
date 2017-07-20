#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate redis;

use bincode::deserialize;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct RedisHashMapVal {
/*	txid:	String,
	index:	usize,*/
	block_height:	usize,
	output_val:	u64,
	address:	String
}

fn main() {
  let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
  let redis_connection = redis_client.get_connection().unwrap();
  let info: redis::InfoDict = redis::cmd("INFO").query(&redis_connection).unwrap();
  if let Some(version) = info.get("redis_version") as Option<String> {
    println!("Connected to Redis v{} at redis://localhost:6379", version);
  } else {
    println!("Connected to Redis with unknown version at redis://localhost:6379");
  }

  let iter: redis::Iter<(String, Vec<u8>)> = redis::cmd("HSCAN").arg("bitcoin_unspent").cursor_arg(0).iter(&redis_connection).unwrap();
  for (key, encoded) in iter {
    let value: RedisHashMapVal = deserialize(&encoded).unwrap();
    let txid = &key[0..63];
    let index = &key[64..key.len()-1];

    println!("{};{};{};{};{}","txid","indexOut","height","value","address");
    println!(
	"{};{};{};{};{}",
	txid,
	index,
	value.block_height,
	value.output_val,
	value.address
    );
  }
}
