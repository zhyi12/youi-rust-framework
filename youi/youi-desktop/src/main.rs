extern crate youi_dataframe;

use youi_dataframe::df_script_executor;

pub fn main(){

    let json_str = df_script_executor("
        let result = readCsv(\"/Volumes/D/data/local/ac/X204-1.50.00.csv\")
            .groupBy(\"count\",\"地(区、市、州、盟)\",\"项目名称\");
        result
    ");

    println!("result: {}", json_str);     // prints 1

}