use std;
use std::fmt::Write as FmtWrite;
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

use postgres::{Connection, TlsMode};

use bincode::{serialize, deserialize, Infinite};

/// Dumps the whole blockchain into csv files
pub struct DbCsvDump {
    // Each structure gets stored in a seperate csv file
    dump_folder:    PathBuf,
    unspent_writer: BufWriter<File>,

    postgres_connection: Connection,

    start_height:   usize,
    end_height:     usize,
    tx_count:       u64,
    in_count:       u64,
    out_count:      u64
}

#[derive(Serialize, Deserialize, Debug)]
struct DbHashMapVal {
/*	txid:	String,
	index:	usize,*/
	block_height:	usize,
	output_val:	u64,
	address:	String
}

impl DbCsvDump {
    fn create_writer(cap: usize, path: PathBuf) -> OpResult<BufWriter<File>> {
        let file = match File::create(&path) {
            Ok(f) => f,
            Err(err) => return Err(OpError::from(err))
        };
        Ok(BufWriter::with_capacity(cap, file))
    }
}

impl Callback for DbCsvDump {

    fn build_subcommand<'a, 'b>() -> App<'a, 'b> where Self: Sized {
        SubCommand::with_name("dbcsvdump")
            .about("Dumps the unspent outputs to CSV file using a database for the state tracking")
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
            let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_owned());
            let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_owned());
            let database = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "rusty_blockchain".to_owned());
            let url = format!("postgres://{}@{}/{}", user, host, database);
            let postgres_connection = try!(Connection::connect(url.clone(), TlsMode::None));
            info!(target: "callback", "Connected to PostgreSQL at {}", url);

            let cap = 4000000;
            let cb = DbCsvDump {
                dump_folder: PathBuf::from(dump_folder),
                unspent_writer: try!(DbCsvDump::create_writer(cap, dump_folder.join("unspent.csv.tmp"))),
                postgres_connection: postgres_connection,
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
        info!(target: "callback", "Using `dbcsvdump` with dump folder: {} ...", &self.dump_folder.display());
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        let trans = self.postgres_connection.transaction().unwrap();
        //let check_statement = trans.prepare("SELECT EXISTS(SELECT 1 FROM results WHERE key = $1) AS \"exists\"").unwrap();
        //let delete_statement = trans.prepare("DELETE FROM results WHERE key = $1").unwrap();
        let insert_statement = trans.prepare("INSERT INTO results (key, encoded) VALUES ($1, $2)").unwrap();

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

            let mut batch_delete_statement = String::new();
            write!(&mut batch_delete_statement, "DELETE FROM results WHERE key IN (").unwrap();

            let mut i = 0;
            for input in &tx.value.inputs {
		let input_outpoint_txid_idx = utils::arr_to_hex_swapped(&input.outpoint.txid) + &input.outpoint.index.to_string();
/*
                //let mut txid = String::new();
                //for &byte in &input.outpoint.txid { write!(&mut txid, "{:x}", byte).unwrap(); }
                //println!("height: {}, txid: {}, idx: {}, input_outpoint_txid_idx: {}", block_height, txid, input.outpoint.index, input_outpoint_txid_idx);
                let val: bool = match check_statement.query(&[&input_outpoint_txid_idx]) {
                    Ok(rows) => {
                      let row = rows.get(0);
                      row.get("exists")
                    },
                    Err(err) => panic!("Error checking key {} {:?}", input_outpoint_txid_idx, err),
                };

                if val {
                  //let mut txid = String::new();
                  //for &byte in &input.outpoint.txid { write!(&mut txid, "{:x}", byte).unwrap(); }
                  //println!("height: {}, txid: {}, idx: {}, input_outpoint_txid_idx: {}", block_height, txid, input.outpoint.index, input_outpoint_txid_idx);
                  delete_statement.execute(&[&input_outpoint_txid_idx]).unwrap();
                }
*/
                if i > 0 {
                  write!(&mut batch_delete_statement, ", '{}'", input_outpoint_txid_idx).unwrap();
                } else {
                  write!(&mut batch_delete_statement, "'{}'", input_outpoint_txid_idx).unwrap();
                }
                i = i + 1;
            }
/*
            let id_delete: Vec<&String> = tx.value.inputs.iter().map(|input| &(utils::arr_to_hex_swapped(&input.outpoint.txid) + &input.outpoint.index.to_string())).collect();
            let id_delete_slice = id_delete.as_slice();
            delete_statement.execute(id_delete_slice);
*/
            write!(&mut batch_delete_statement, ");").unwrap();
            trans.execute(&batch_delete_statement, &[]).unwrap();

            self.in_count += tx.value.in_count.value;

            // serialize outputs
            for (i, output) in tx.value.outputs.iter().enumerate() {
		let hash_val: DbHashMapVal = DbHashMapVal {
			block_height: block_height,
			output_val: output.out.value,
			address: output.script.address.clone(),
			//script_pubkey: utils::arr_to_hex(&output.out.script_pubkey)
		};
                let key = txid_str.clone() + &i.to_string();
                //println!("txid: {}, idx: {}", txid_str, i);
                let encoded: Vec<u8> = serialize(&hash_val, Infinite).unwrap();
                insert_statement.execute(&[&key, &encoded]).unwrap();
            }
            self.out_count += tx.value.out_count.value;
        }
        self.tx_count += block.tx_count.value;

        trans.commit().is_ok();
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
        let query = self.postgres_connection.query("SELECT key, encoded FROM results", &[]);
        let iter = query.iter().map(|row| row.get(0));
        for row in iter {
                let key: String = row.get(0);
                let encoded: Vec<u8> = row.get(1);
                let value: DbHashMapVal = deserialize(&encoded).unwrap();
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
