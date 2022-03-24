

fn main(){

    let database_url = String::from("");
    youi_sqlite::create_sqlite_pool(&database_url);

}