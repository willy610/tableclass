use crate::table::OperLog;
use std::collections::HashMap;

static mut KKK: Option<HashMap<i32, OperLog>> = None;
static mut DOLOGG: bool = true;
static mut DOLOGGDISABLED: bool = false;
static mut LOGGAID: i32 = 0;

pub fn _logit_get_size() {
    unsafe {
        match &KKK {
            Some(dict) => {
                eprintln!("KKK.len()={}", dict.len());
            }
            None => {}
        }
    }
}
pub fn logit_gen_new_id() -> i32 {
    unsafe {
        LOGGAID = LOGGAID + 1;
        LOGGAID
    }
}
/*_____________________________________________________________*/
pub fn logit_reset(on_is_true: bool) {
    unsafe {
        if on_is_true {
            DOLOGG = true;
            KKK = Some(HashMap::new());
        } else {
            DOLOGG = false;
        }
    }
}
/*_____________________________________________________________*/
pub fn logit_disable() {
    unsafe {
        DOLOGGDISABLED = true;
    }
}
/*_____________________________________________________________*/
pub fn logit_enable() {
    unsafe {
        DOLOGGDISABLED = false;
    }
}
/*_____________________________________________________________*/
pub fn is_logit_enabled() -> bool {
    unsafe { DOLOGGDISABLED == false }
}
/*_____________________________________________________________*/
pub fn logit_dump() {
    unsafe {
        eprintln!("{:#?}", KKK);
    }
}
/*_____________________________________________________________*/
pub fn logit_get_value(key: i32) -> Box<OperLog> {
    unsafe {
        if let Some(the_dict) = &KKK {
            let x = the_dict.get(&key);
            let z = match x {
                Some(y) => Box::new(y.clone()),
                None => {
                    panic!("logit_get_value not found {}", key);
                }
            };
            return z;
        } else {
            panic!("logit_get_value not found {}", key);
        }
    }
}
/*_____________________________________________________________*/
pub fn logit_collect_operlog_single(did: OperLog, id_new_table: i32) {
    unsafe {
        if DOLOGGDISABLED == false {
            if let Some(ref mut the_dict) = KKK {
                if the_dict.contains_key(&id_new_table) {
                    panic!("Key already exist {}", id_new_table);
                }
                the_dict.insert(id_new_table, did);
            }
        }
    }
}
