use sqlx::{Error, Pool, Sqlite};
use youi_geo::area::PolygonGrid;

pub mod task;
pub mod json;

///
/// 返回顶级任务集合的geo格式数据
///
pub async fn area_geo_task(pool:&Pool<Sqlite>,area_id:&str)->Result<String,Error>{
    let top_tasks = task::find_area_tasks(pool,area_id,1).await?;

    Ok(json::geo_tasks(top_tasks))
}

///
/// 返回子任务集合的geo格式数据
///
pub async fn area_geo_sub_task(pool:&Pool<Sqlite>,task_id:i32)->Result<String,Error>{
    let opt_task = task::find_task(&pool,task_id).await?;

    match opt_task{
        None => {
        }
        Some(task) => {
            let sub_tasks = task::find_sub_tasks(pool,&task.area_id,task_id).await?;
            return Ok(json::geo_tasks(sub_tasks));
        }
    }

    Ok(String::from(""))
}

///
/// 展开子任务
///
pub async fn expand_sub_task(pool:&Pool<Sqlite>,task_id:i32)->Result<String,Error>{
    let opt_task = task::find_task(&pool,task_id).await?;
    //获取子任务
    match opt_task{
        None => {
            Ok(String::from(format!("error:task {} not find.",task_id)))
        }
        Some(task) => {
            let sub_count = task::sub_tasks_count(&pool,task_id).await?;
            if sub_count==0 && task.count > 99 {
                //重新切分
                let poly = youi_geo::area::point_to_poly(task.x,task.y,task.distance);
                //根据count数量切分区域
                let distance = task.distance/5.;
                let grid:PolygonGrid = youi_geo::area::find_poly_grid(&poly,0,0,distance);
                let sub_level = task.level+1;
                //存储任务到数据库
                task::save_tasks(pool,&task.area_id,&poly,&grid,distance,task.id,sub_level).await?;
                return Ok(String::from("success"));
            }
            Ok(String::from("nothing"))
        }
    }
}


