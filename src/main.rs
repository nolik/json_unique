use std::fs::File;
use std::io::{BufRead, BufReader};

use serde_json::Value;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

fn main() {
    println!("Start calculation!");
    let filename = "/home/nolik/downloads/";
    // Open the file in read-only mode (ignoring errors).
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);

    let mut msg_stats: HashMap<u64, Vec<String>> = HashMap::new();

    // Read the file line by line using the lines() iterator from std::io::BufRead.
    for (index, line) in reader.lines().enumerate() {
        println!("{:?}", index + 1);
        let line = line.unwrap(); // Ignore errors.
        // Show the line and its number.
        //        println!("{}. {}", index + 1, line);
        let message: Value = serde_json::from_str(&line).unwrap();
        let intr = message["msg"]["intr"].as_bool();

        if intr.unwrap() {
            let apn_prefix = message["msg"]["apn_ref"]["pf"].as_str();
            let apn_base = message["msg"]["apn_ref"]["bs"].as_str();
            let apn_suffix = message["msg"]["apn_ref"]["_sf"].as_str();
            let hier_ref = message["msg"]["hier_cd_ref"].as_str();
            let hash: u64 = calculate_hash(&apn_prefix, &apn_base, &apn_suffix, &hier_ref);

            let vec_of_similar_part = msg_stats.get_mut(&hash);

            match vec_of_similar_part {
                Some(v) => v.push(line),
                None => {
                    msg_stats.insert(hash, vec![line]);
                }
            }
        }
    }

    let result = msg_stats
        .values()
        .filter(|vec| vec.len() > 1)
        .flat_map(|vec| &vec[1..])
        .count();
    println!("/n duplicated {:?}", result);
}

//
//#[derive(Deserialize, Debug)]
//struct KafkaMessage {
//    meta: String,
//    msg: String,
//}

fn calculate_hash<T: Hash>(t: &T, d: &T, c: &T, f: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    d.hash(&mut s);
    c.hash(&mut s);
    f.hash(&mut s);
    s.finish()
}
