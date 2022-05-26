# 题目一
## 展示电影 ID 为 2116 这部电影各年龄段的平均影评分。
```
select t_user.age, avg(t_rating.rate) from t_user right join t_rating on t_user.userid=t_rating.userid where t_rating.movieid=2116 group by t_user.age;
```

# 题目二
## 找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。
```
select t_user.sex,t_movie.moviename,avg(t_rating.rate),count(t_rating.userid) from t_user full join t_rating on t_user.userid=t_rating.userid full join t_movie on t_rating.movieid=t_movie.movieid where t_user.sex='M' group by t_user.sex, t_movie.moviename having count(t_rating.userid) > 50 order by avg(t_rating.rate) desc limit 10;
```

# 题目三（选做）
## 找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）
```
select t_movie.moviename, avg(t_rating.rate) from t_movie full join t_rating on t_movie.movieid=t_rating.movieid where t_movie.movieid in (select t_rating.movieid from t_rating where t_rating.userid in (select t_user.userid from t_user full join t_rating on t_user.userid=t_rating.userid where t_user.sex='F' group by t_user.userid order by count(t_rating.movieid) desc limit 1) group by t_rating.movieid order by max(t_rating.rate) desc limit 10) group by t_movie.moviename limit 10;
```

### 思路
* 先找出影评次数最多的女性的userid: select t_user.userid from t_user full join t_rating on t_user.userid=t_rating.userid where t_user.sex='F' group by t_user.userid order by count(t_rating.movieid) desc limit 1
* 再列出给出最高分的10部电影的movieid：select t_rating.movieid from t_rating where t_rating.userid in (select t_user.userid from t_user full join t_rating on t_user.userid=t_rating.userid where t_user.sex='F' group by t_user.userid order by count(t_rating.movieid) desc limit 1) group by t_rating.movieid order by max(t_rating.rate) desc limit 10
* 再查询这10部电影的名称，以及平均影评分：select t_movie.moviename, avg(t_rating.rate) from t_movie full join t_rating on t_movie.movieid=t_rating.movieid where t_movie.movieid in (select t_rating.movieid from t_rating where t_rating.userid in (select t_user.userid from t_user full join t_rating on t_user.userid=t_rating.userid where t_user.sex='F' group by t_user.userid order by count(t_rating.movieid) desc limit 1) group by t_rating.movieid order by max(t_rating.rate) desc limit 10) group by t_movie.moviename limit 10;
