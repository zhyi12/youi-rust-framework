use serde::Serialize;
use PagerRecords;

///
/// 结果集输出为json
///
pub fn  records_to_json<T: Sized+Serialize>(records:Vec<T>) -> String{
    //
    let result = PagerRecords{ records };
    //
    let json_str = serde_json::to_string(&result);
    //
    json_str.unwrap()
}