use std::fs::{self, File};
use std::path::PathBuf;
use std::io::{BufWriter, Write};
use std::clone::Clone;

use clap::{Arg, ArgMatches, App, SubCommand};

use callbacks::Callback;
use errors::{OpError, OpResult};

use blockchain::parser::types::CoinType;
use blockchain::proto::block::Block;
use blockchain::utils;

//use redis::{self, Commands, ToRedisArgs, FromRedisValue, RedisResult, Value};
use redis::{self, Commands};

use bincode::{serialize, deserialize, Infinite};

/// Dumps the whole blockchain into csv files
pub struct RedisCsvDump {
    // Each structure gets stored in a seperate csv file
    dump_folder:    PathBuf,
    unspent_writer: BufWriter<File>,

    redis_connection: redis::Connection,

    start_height:   usize,
    end_height:     usize,
    tx_count:       u64,
    in_count:       u64,
    out_count:      u64
}

#[derive(Serialize, Deserialize, Debug)]
struct RedisHashMapVal {
/*	txid:	String,
	index:	usize,*/
	block_height:	usize,
	output_val:	u64,
	address:	String
}

impl RedisCsvDump {
    fn create_writer(cap: usize, path: PathBuf) -> OpResult<BufWriter<File>> {
        let file = match File::create(&path) {
            Ok(f) => f,
            Err(err) => return Err(OpError::from(err))
        };
        Ok(BufWriter::with_capacity(cap, file))
    }
}

impl Callback for RedisCsvDump {

    fn build_subcommand<'a, 'b>() -> App<'a, 'b> where Self: Sized {
        SubCommand::with_name("rediscsvdump")
            .about("Dumps the unspent outputs to CSV file using Redis")
            .version("0.1")
            .author("mbilker <me@mbilker.us>")
            .arg(Arg::with_name("dump-folder")
                .help("Folder to store csv file")
                .index(1)
                .required(true))
    }

    fn new(matches: &ArgMatches) -> OpResult<Self> where Self: Sized {
        let ref dump_folder = PathBuf::from(matches.value_of("dump-folder").unwrap()); // Save to unwrap
        match (|| -> OpResult<Self> {
            let redis_client = try!(redis::Client::open("redis://luna.lab.mbilker.us:6379"));
            let redis_connection = try!(redis_client.get_connection());
            let info: redis::InfoDict = try!(redis::cmd("INFO").query(&redis_connection));
            if let Some(version) = info.get("redis_version") as Option<String> {
              info!(target: "callback", "Connected to Redis v{} at redis://luna.lab.mbilker.us:6379", version);
            } else {
              info!(target: "callback", "Connected to Redis with unknown version at redis://luna.lab.mbilker.us:6379");
            }

            let cap = 4000000;
            let cb = RedisCsvDump {
                dump_folder: PathBuf::from(dump_folder),
                unspent_writer: try!(RedisCsvDump::create_writer(cap, dump_folder.join("unspent.csv.tmp"))),
                redis_connection: redis_connection,
                start_height: 0, end_height: 0, tx_count: 0, in_count: 0, out_count: 0
            };
            Ok(cb)
        })() {
            Ok(s) => return Ok(s),
            Err(e) => return Err(
                tag_err!(e, "Couldn't initialize csvdump with folder: `{}`", dump_folder
                        .as_path()
                        .display()))
        }
    }

    fn on_start(&mut self, _: CoinType, block_height: usize) {
        self.start_height = block_height;
        info!(target: "callback", "Using `rediscsvdump` with dump folder: {} ...", &self.dump_folder.display());
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        // serialize transaction
        for tx in block.txs {
	    // For each transaction in the block,
	    // 1. apply input transactions (remove (TxID == prevTxIDOut and prevOutID == spentOutID))
	    // 2. apply output transactions (add (TxID + curOutID -> HashMapVal))
	    // For each address, retain:
	    // * block height as "last modified"
	    // * output_val
	    // * address

            //self.tx_writer.write_all(tx.as_csv(&block_hash).as_bytes()).unwrap();
            let txid_str = utils::arr_to_hex_swapped(&tx.hash);

            for input in &tx.value.inputs {
		let input_outpoint_txid_idx = utils::arr_to_hex_swapped(&input.outpoint.txid) + &input.outpoint.index.to_string();
                //let val: bool = match self.redis_connection.hexists("bitcoin_unspent", &input_outpoint_txid_idx) {
                //    Ok(v) => v,
                //    Err(err) => panic!("Error checking key {} {:?}", input_outpoint_txid_idx, err),
                //};

                //if val {
                match self.redis_connection.hdel::<_,_,i32>("bitcoin_unspent", &input_outpoint_txid_idx) {
                  Ok(_) => (),
                  Err(err) => panic!("Error deleting hash entry {} {:?}", input_outpoint_txid_idx, err),
                };
		//};
            }
            self.in_count += tx.value.in_count.value;

            // serialize outputs
            for (i, output) in tx.value.outputs.iter().enumerate() {
		let hash_val: RedisHashMapVal = RedisHashMapVal {
			block_height: block_height,
			output_val: output.out.value,
			address: output.script.address.clone(),
			//script_pubkey: utils::arr_to_hex(&output.out.script_pubkey)
		};
                let key = txid_str.clone() + &i.to_string();
                let encoded: Vec<u8> = serialize(&hash_val, Infinite).unwrap();
                match self.redis_connection.hset::<_,_,_,i32>("bitcoin_unspent", &key, encoded) {
                  Ok(_) => (),
                  Err(err) => panic!("Error setting hash entry {} = {:?} (err: {:?})", key, hash_val, err),
                };
            }
            self.out_count += tx.value.out_count.value;
        }
        self.tx_count += block.tx_count.value;
    }

    fn on_complete(&mut self, block_height: usize) {
        self.end_height = block_height;

	self.unspent_writer.write_all(format!(
		"{};{};{};{};{}\n",
		"txid",
		"indexOut",
		"height",
		"value",
		"address"
		).as_bytes()
	).unwrap();
        let iter: redis::Iter<(String, Vec<u8>)> = redis::cmd("HSCAN").arg("bitcoin_unspent").cursor_arg(0).iter(&self.redis_connection).unwrap();
        for (key, encoded) in iter {
                let value: RedisHashMapVal = deserialize(&encoded).unwrap();
		let txid = &key[0..63];
		let index = &key[64..key.len()-1];
		//let  = key.len();
		//let mut mut_key = key.clone();
		//let index: String = mut_key.pop().unwrap().to_string();
		self.unspent_writer.write_all(format!(
				"{};{};{};{};{}\n",
				txid,
				index,
				value.block_height,
				value.output_val,
				value.address
			).as_bytes()
		).unwrap();
	}

        // Keep in sync with c'tor
        for f in vec!["unspent"] {
            // Rename temp files
            fs::rename(self.dump_folder.as_path().join(format!("{}.csv.tmp", f)),
                       self.dump_folder.as_path().join(format!("{}-{}-{}.csv", f, self.start_height, self.end_height)))
                .expect("Unable to rename tmp file!");
        }

        info!(target: "callback", "Done.\nDumped all {} blocks:\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, self.tx_count, self.in_count, self.out_count);
    }
}
