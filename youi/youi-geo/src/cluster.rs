use std::collections::HashMap;
use geo::{Coordinate, point, Point, Polygon, polygon};
use geo::prelude::*;
use geojson::GeoJson;
use num_traits::ToPrimitive;
use smartcore::cluster::kmeans::{KMeans, KMeansParameters};
use smartcore::linalg::BaseMatrix;
use smartcore::linalg::naive::dense_matrix::DenseMatrix;
use youi_dataframe::{ChunkCompare, DataFrame};
use crate::{AddressArea, AddressPoint};
use crate::json::to_geo_json;

pub struct DfClusterParameters{
    pub group_size:i32,
    pub split_distance:f64
}

///
///
/// df columns{经度,纬度,count}
/// 地图数据聚合
///
pub fn geo_df_cluster(df:&DataFrame,poly:&Polygon<f64>,options:&DfClusterParameters)->String{
    //
    let bound = poly.bounding_rect().unwrap();

    let max_coord = bound.max();
    let min_coord = bound.min();
    let x_split_count:i32 = ((max_coord.x - min_coord.x)/options.split_distance).ceil() as i32;
    let y_split_count:i32 = ((max_coord.y - min_coord.y)/options.split_distance).ceil() as i32;

    //提取大于options.group_size的地址数据
    let filter = df.column("count").unwrap().gt_eq(options.group_size);
    let df_gt_group = df.filter(&filter).unwrap();
    //
    println!("{:?}",df_gt_group.get_columns());

    //切分待处理区域
    let address_areas = split_address_areas(df,&min_coord,x_split_count,y_split_count,options);

    //推算需要分区的个数
    let all_count:i32 = address_areas.iter().map(|address_area|address_area.count).sum();
    let group_count:i32 = all_count/options.group_size+1;
    println!("共{}个调查对象，预计分区个数：{}",all_count,group_count);
    //区域聚类
    let group_area_map:HashMap<String,Vec<AddressArea>> = to_group_area_map(&address_areas,group_count);

    let mut centroids:Vec<Vec<f64>> = Vec::new();
    let mut all_points:Vec<Vec<f64>> = Vec::new();
    group_area_map.iter().for_each(|entry|{
        //开始位置
        let start = all_points.len();
        //区域计数
        let area_count_sum:i32 = entry.1.iter().map(|address_area|address_area.count).sum();
        //add to all_points
        entry.1.iter().for_each(|area|area.points.iter().for_each(|point|{
            for _ in 0..point.count{
                all_points.push(vec![point.lng,point.lat]);
            }
        }));
        if area_count_sum>options.group_size/3{
            //质点个数
            let centroid_count:f64 = (area_count_sum as f64/options.group_size as f64).ceil();

            println!("{:?} {},质点个数：{} ",entry.0,area_count_sum,centroid_count);
            //
            for i in 0..centroid_count.to_i32().unwrap(){
                let idx:usize = (i*options.group_size).to_usize().unwrap();
                let idx_point = all_points.get(start+idx).unwrap();
                centroids.push(idx_point.clone());
            }
        }
    });

    println!("{:?}",centroids);

    let matrix_centroids = DenseMatrix::from_2d_vec(&centroids);
    let matrix_points = DenseMatrix::from_2d_vec(&all_points);

    let k_means_point = KMeans::fit(&matrix_centroids, KMeansParameters::default()
        .with_k(centroids.len()))
        .unwrap(); // Fit to data, 2 clusters
    let result = k_means_point.predict(&matrix_points).unwrap(); // use the same points for prediction

    //
    let mut k_area_points:HashMap<String,Vec<Point<f64>>> = HashMap::new();

    //
    let mut address_points:Vec<AddressPoint> = Vec::with_capacity(result.len());
    for i in 0..result.len(){
        let area_key = result[i].to_string();

        if !k_area_points.contains_key(&area_key){
            k_area_points.insert(String::from(&area_key), Vec::new());
        }
        let row:Vec<f64> = matrix_points.get_row_as_vec(i);
        let x = row.get(0).unwrap().clone();
        let y = row.get(1).unwrap().clone();
        let point = point!(x:x,y:y);
        k_area_points.get_mut(&area_key).unwrap().push(point);
        address_points.push(AddressPoint{
            lng: x,
            lat: y,
            count: 0,
            group: result[i] as i32
        });
    }

    let json:GeoJson = to_geo_json(&k_area_points,&address_points);

    json.to_string()
}

///
/// 切分待处理区域
///
fn split_address_areas(df:&DataFrame,min_coord:&Coordinate<f64>,x_split_count:i32,y_split_count:i32,options:&DfClusterParameters) -> Vec<AddressArea>{
    let mut address_areas:Vec<AddressArea> = Vec::new();
    //
    for dx in 1..x_split_count {
        for dy in 1..y_split_count {
            let index: i32 = dx * x_split_count + dy;
            let split_poly:Polygon<f64> = build_split_poly(dx,dy,options.split_distance,&min_coord);

            let address_points = find_split_poly_points(&df,&split_poly,options.group_size);

            if address_points.len()>0 {
                let count:i32 = address_points.iter().map(|address_point|address_point.count).sum();

                if count>0{
                    //待合并区域
                    let rect = split_poly.bounding_rect().unwrap();
                    println!("{},{}-{}, {},{:?}",index,dx,dy,count,rect);
                    address_areas.push(AddressArea{
                        index,
                        x: dx,
                        y: dy,
                        count,
                        rect,
                        points:address_points,
                        group:-1.
                    });
                }
            }
        }
    }

    address_areas
}

///
///
///
fn build_split_poly(dx:i32,dy:i32,split_distance:f64,min_coord:&Coordinate<f64>) ->Polygon<f64>{
    let left: f64 = min_coord.x + (dx as f64 - 1.0) * split_distance;
    let right: f64 = min_coord.x + dx as f64 * split_distance;
    let top: f64 = min_coord.y + (dy as f64 - 1.0) * split_distance;
    let bottom: f64 = min_coord.y + dy as f64 * split_distance;

    polygon![
                 (x:  left, y: top),
                 (x:  right, y: top),
                 (x: right, y: bottom),
                 (x:  left, y: bottom),
            ]
}
///
/// 查找区域内的点集合
///
fn find_split_poly_points(df:&DataFrame,poly:&Polygon<f64>,group_size:i32)->Vec<AddressPoint>{
    let mut address_points = Vec::new();

    let size = df.height();

    for i in 0..size {
        let row_vec = df.get(i).unwrap();
        let lng:f64 = row_vec.get(0).unwrap().extract().unwrap();//经度
        let lat:f64 = row_vec.get(1).unwrap().extract().unwrap();//纬度
        let cnt:u32 = row_vec.get(2).unwrap().extract().unwrap();//经纬度所在地点的对象数量

        let point = point!{x:lng,y:lat};

        //包含的点
        let point_contains = poly.contains(&point);
        //TODO 边线上的点
        if point_contains{
            let count:i32 = cnt.to_i32().unwrap()%group_size;
            if count>0{
                address_points.push(AddressPoint{
                    lng,
                    lat,
                    count,
                    group:0
                });
            }
        }
    }

    address_points
}

///
/// k-means 区域聚类
///
fn to_group_area_map(address_areas:&Vec<AddressArea>,group_count:i32)->HashMap<String,Vec<AddressArea>>{

    let arr:Vec<Vec<f64>> = address_areas.iter().map(|address_area|
        vec![address_area.x as f64,address_area.y as f64])
        .collect();
    //
    let matrix = DenseMatrix::from_2d_vec(&arr);
    // //
    let kmeans = KMeans::fit(&matrix, KMeansParameters::default().with_k(group_count as usize)).unwrap(); // Fit to data, 2 clusters
    let y_hat = kmeans.predict(&matrix).unwrap(); // use the same points for prediction

    let mut group_area_map:HashMap<String,Vec<AddressArea>> = HashMap::new();

    for i in 0..arr.len(){
        let address_area = address_areas.get(i).unwrap();
        let group = y_hat.get(i).unwrap().clone();
        let key = group.to_string();
        let areas = Vec::new();
        if !group_area_map.contains_key(&key){
            group_area_map.insert(key,areas);
        }

        group_area_map.get_mut(&group.to_string()).unwrap().push(AddressArea{
            index: address_area.index,
            x: address_area.x,
            y: address_area.y,
            count: address_area.count,
            rect: address_area.rect.clone(),
            points: address_area.points.iter().map(|point|AddressPoint{
                lng: point.lng,
                lat: point.lat,
                count: point.count,
                group: point.group,
            }).collect(),
            group
        });
    }

    group_area_map
}