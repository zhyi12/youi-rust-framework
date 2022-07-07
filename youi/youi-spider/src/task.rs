//! #spider task
//! 互联网企业数据获取
//!
//!
//!

use geo::Polygon;
use sqlx::{Error, Pool, Row, Sqlite};
use sqlx::sqlite::SqliteRow;
use youi_geo::area::{GridPoint, PolygonGrid};

///
///
///
#[derive(Debug)]
pub struct GeoSpiderTask{
    pub id:i32,
    pub x:f64,
    pub y:f64,
    pub r:f64,
    pub distance:f64,
    pub count:i32,
    pub page:i32,
    pub current:i32,
    pub level:i32,
    pub status:i32,
    pub area_id:String
}

///
///
///
pub enum SpiderMessage{
    ///
    /// 启动区块记录总数抓取
    StartFetchCount(Vec<i32>),
    Fetch(i32),
    Update,
    Complete
}
///
/// 获取区县及以下级别企业数据
///
pub async fn area_respondent_spider(pool:&Pool<Sqlite>,area_id:&str) -> Result<SpiderMessage,Error>{

    let top_tasks = find_area_tasks(pool,area_id,1).await?;
    //1-检查是否需要生成spider task
    if top_tasks.is_empty(){
        init_task(pool,area_id).await?;
    }
    //找出需要fetch count的任务,包括未获取count和当前页数少于总页数两种情况
    let need_fetch_count_tasks:Vec<i32> = find_fetch_count_tasks(top_tasks);

    //2- 检查记录总数获取情况，如果存在未获取的，返回发送启动互联网获取任务的记录总数的消息
    if !need_fetch_count_tasks.is_empty(){
        return Ok(SpiderMessage::StartFetchCount(need_fetch_count_tasks));
    }

    //恢复中断的任务，count已经获取且current<page
    Ok(SpiderMessage::Complete)
}

fn find_fetch_count_tasks(tasks:Vec<GeoSpiderTask>)->Vec<i32>{
    tasks.iter().filter(|task|{
        if task.count>-1 && task.page == task.current{
            false
        }else{
            true
        }
    }).map(|task|task.id).collect()
}

///
/// 初始化任务
///
async fn init_task(pool:&Pool<Sqlite>,area_id:&str)->Result<(),Error>{
    println!("init {} task.",area_id);
    let geo_json = youi_geo::query::find_area_geo_json(pool,area_id).await.unwrap();

    let poly = youi_geo::area::find_main_poly(geo_json);

    let distance = 0.008;
    let poly_grid = youi_geo::area::find_poly_grid(&poly,0,0,distance);
    println!("total {}",poly_grid.total);
    //
    save_tasks(pool,area_id,&poly,&poly_grid,distance,-1,1).await?;

    Ok(())
}

///
/// 保存任务集合
///
pub async fn save_tasks(pool:&Pool<Sqlite>,area_id:&str,poly:&Polygon<f64>,poly_grid:&PolygonGrid,distance:f64,pid:i32,level:i32)->Result<(),Error>{
    for i in 0..poly_grid.x_block{
        for j in 0..poly_grid.y_block{
            let cur_poly_grid = youi_geo::area::find_poly_grid(poly,i,j,distance);
            if !cur_poly_grid.grids.is_empty(){
                for idx in 0..cur_poly_grid.grids.len(){
                    let grid_point = cur_poly_grid.grids.get(idx).unwrap();
                    save_task(pool,area_id,&grid_point,distance,pid,level).await?;
                }
            }
        }
    }
    Ok(())
}
///
/// 保存任务
///
pub async fn save_task(pool:&Pool<Sqlite>,area_id:&str,grid_point:&GridPoint,distance:f64,pid:i32,level:i32)->Result<(),Error>{
    sqlx::query("insert into stats_area_spider_task(area_id,x,y,r,d,level,status,count,pid) values(?1,?2,?3,?4,?5,?6,?7,?8,?9)" )
            .bind(area_id)
            .bind(grid_point.x)
            .bind(grid_point.y)
            .bind(grid_point.r)
            .bind(distance)
            .bind(level)
            .bind(0)
            .bind(-1)
            .bind(pid)
        .execute(pool).await?;
    Ok(())
}

///
/// 任务列表查询
/// 
pub async fn find_area_tasks(pool:&Pool<Sqlite>,area_id:&str,level:i32)->Result<Vec<GeoSpiderTask>,Error>{
    let tasks_result = sqlx::query("select id,x,y,r,d,count,page,current,level,status from stats_area_spider_task where area_id=?1 and level=?2 order by id")
        .bind(area_id).bind(level)
        .map(|row|row_map_task(row,area_id))
        .fetch_all(pool).await?;
    Ok(tasks_result)
}

///
///
///
pub async fn find_task(pool:&Pool<Sqlite>,task_id:i32)->Result<Option<GeoSpiderTask>,Error>{
    let result = sqlx::query("select id,area_id,x,y,r,d,count,page,current,level,status from stats_area_spider_task where id=?1")
        .bind(task_id).fetch_optional(pool).await?;

    if result.is_some(){
        let row = result.unwrap();
        let area_id = row.get::<String, &str>("area_id");
        return Ok(Some(row_map_task(row,&area_id)));
    }

    Ok(None)
}

///
///
///
pub async fn sub_tasks_count(pool:&Pool<Sqlite>,task_id:i32)->Result<i32,Error>{
    let mut count = 0;
    let res= sqlx::query("select count(1) as count from stats_area_spider_task where pid=?1")
        .bind(task_id).fetch_optional(pool).await?;

    if res.is_some(){
        count = res.unwrap().get::<i32, &str>(&"count");
    }
    Ok(count)
}
///
///
///
pub async fn find_sub_tasks(pool:&Pool<Sqlite>,area_id:&str,task_id:i32)->Result<Vec<GeoSpiderTask>,Error>{
    let tasks_result = sqlx::query("select id,x,y,r,d,count,page,current,level,status from stats_area_spider_task where pid=?1 order by id")
        .bind(task_id)
        .map(|row|row_map_task(row,area_id))
        .fetch_all(pool).await?;
    Ok(tasks_result)
}

///
///
///
fn row_map_task(row:SqliteRow,area_id:&str)->GeoSpiderTask{
    GeoSpiderTask {
        id: row.get::<i32, &str>(&"id"),
        area_id: area_id.to_string(),
        x: row.get::<f64, &str>(&"x"),
        y: row.get::<f64, &str>(&"y"),
        r: row.get::<f64, &str>(&"r"),
        distance: row.get::<f64, &str>(&"d"),
        count: row.get::<i32, &str>(&"count"),
        page: row.get::<i32, &str>(&"page"),
        current: row.get::<i32, &str>(&"current"),
        level: row.get::<i32, &str>(&"level"),
        status: row.get::<i32, &str>(&"status")
    }
}

///
///
///
pub async fn clear_area_tasks(pool:&Pool<Sqlite>,area_id:&str)->Result<(),Error>{
    sqlx::query("delete from stats_area_spider_task where area_id=?1")
        .bind(area_id).execute(pool).await?;
    Ok(())
}