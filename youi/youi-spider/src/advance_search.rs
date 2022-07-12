use sqlx::{Pool, Sqlite,query,Row,Error};

#[derive(Clone,Debug,serde::Deserialize,serde::Serialize)]
pub struct AdvanceFilter{
    id:i32,
    start_year:i32,
    end_year:i32,
    start_reg_cap:i32,
    end_reg_cap:i32,
    total:i32
}

///
///
///
pub async fn find_advance_searches(pool:&Pool<Sqlite>,area_id:&str)->Result<Vec<Vec<AdvanceFilter>>,Error>{

    let result = query("select id,start_year,end_year,start_reg_cap,end_reg_cap,total from stats_area_advance_search where area_id=?1 order by total desc")
        .bind(area_id).fetch_all(pool).await?;

    let filters:Vec<AdvanceFilter> = result.iter().map(|row|
        AdvanceFilter{
            id: row.get::<i32,&str>("id"),
            start_year: row.get::<i32,&str>("start_year"),
            end_year: row.get::<i32,&str>("end_year"),
            start_reg_cap: row.get::<i32,&str>("start_reg_cap"),
            end_reg_cap: row.get::<i32,&str>("end_reg_cap"),
            total: row.get::<i32,&str>("total"),
        }
    ).collect();

    let limit = 10000;

    let mut result:Vec<Vec<AdvanceFilter>> = filters.iter().filter(|filter|filter.total>=limit)
        .map(|f|vec![f.clone()]).collect();

    let len = filters.len();
    let mut end = len-1;

    for i in 0..len{
        let filter = &filters[i];
        if filter.total<=limit{
            let mut bags:Vec<AdvanceFilter> = Vec::new();
            bags.push(filter.clone());

            if i >= end{
                println!("{:?}",filter);
                break;
            }

            for j in 0..end{
                let end_filter = &filters[end - j];
                let cur_sum:i32 = bags.iter().map(|f|f.total).sum();
                if cur_sum+end_filter.total < limit && bags.len()<6{
                    bags.push(end_filter.clone());
                }else{
                    end = end-j;
                    break;
                }
            }

            result.push(bags);
        }
    }

    result.iter().for_each(|v|{
        let sum:i32 = v.iter().map(|f|f.total).sum();
        let ids:Vec<i32> = v.iter().map(|f|f.id).collect();
        println!("{} {} {:?}", sum,v.len(),ids);
    });

    let count:usize = result.iter().map(|r|r.len()).sum();
    println!("{} {} {}", result.len(),count,len);
    Ok(result)
}

