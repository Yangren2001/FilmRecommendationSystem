create database movie_data character set utf8;

use  movie_data;

# 建立影视数据表
create table movies(
    mid int primary key ,
    name varchar(128) unique ,
    timelong varchar(128),
    issue varchar(256),
    shoot varchar(256),
    language varchar(128),
    descri varchar(1024),
    actors varchar(4096),
    directors varchar(1024),
    score float default 0.0,
    genres varchar(256)
);

# 用户表
create table users(
  uid int primary key,
  user_name char(40) unique,
  password varchar(64) default '123456',
  first bool default FALSE,
  sex varchar(40),
  occupation varchar(256),
  age varchar(128),
  create_time timestamp default now()
);

# 创建评分表
create table ratings(
    uid int references users(uid),
    mid int references movies(mid),
    rating float default 5.0,
    rating_time timestamp default NOW(),
    primary key (uid, mid)
);

# 用户偏爱类别
create table  u_like(
    create_time datetime primary key default now(),
    uid int references users(uid),
    genres varchar(256)  /*类别*/
    /*rank_score float  偏爱程度*/
);

# 电影标签 用户收藏设置标签
create table tag(
    tid int primary key ,
    uid int references users(uid),
    mid int references movies(mid),
    tag varchar(256),
    time timestamp default NOW()
);

# 电影评分表
create table count_rating(
    mid int primary key ,
    score float default 0.0,
    count int default 0,
    constraint count_rating_fk1 foreign key (mid) references movies(mid)
);

# # 电影排序表
# create table top_movies(
#     uid int references users(uid),
#     mid int references movies(mid),
#     tag varchar(256),
#     time timestamp default now(),
#     primary key (uid, mid)
# );

# # 用户推荐表
# create table user_recs(
#     uid int references users(uid),
#     smid int references movies(mid),
#     score float,   /*电影评分*/
#     primary key (uid, smid)
# );

# 用户实时推荐表
create table streamRecs(
    uid int,
    recs varchar(4096)
);

# # 电影相似表
# create table movie_recs(
#     mid int references movies(mid),
#     smid int references movies(mid),
#     score float,   /*电影评分*/
#     primary key (mid, smid)
# );

# allow create function
set global log_bin_trust_function_creators = 1;

# 建立索引提高查询速度
create index MOVIES_SCORE on movies(score);
create index MOVIES_RATING on ratings(rating);

# 建立触发器
delimiter //
-- DROP FUNCTION if exists random_id//
create function random_id()
returns int
begin
    set @flag = true;
	set @id = "";
	set @i = 0;
	while @i < 9 do
		set @rand_ = cast(floor(rand() * 10) as char);
		set @id =  concat(@id, @rand_, "");
		set @i = @i + 1;
	end while;
    return @id;
end//
delimiter ;

delimiter //
create trigger id_user
before insert
on users
for each row
begin
    if new.uid is null
    then
         set  @id = random_id();
        set @flag = @id in (select uid from users);
        while @flag do
            set @id = random_id();
           set @flag = @id in (select uid from users);
        end while;
        set new.uid = @id;
    end if;
end//
delimiter ;
# drop trigger id_users;

delimiter //
create trigger id_movies
before insert
on movies
for each row
begin
    if new.mid is null
    then
         set  @id = random_id();
        set @flag = @id in (select mid from movies);
        while @flag do
            set @id = random_id();
           set @flag = @id in (select mid from movies);
        end while;
        set new.mid = @id;
    end if;
end//
delimiter ;

delimiter //
create trigger id_tags
before insert
on tag
for each row
begin
    if new.tid is null
    then
         set  @id = random_id();
        set @flag = @id in (select tid from tag);
        while @flag do
            set @id = random_id();
           set @flag = @id in (select tid from tag);
        end while;
        set new.tid = @id;
    end if;
end//
delimiter ;

delimiter //
create trigger rating_insert
after insert
on ratings
for each row
begin
    update movie_data.count_rating set movie_data.count_rating.score=(new.rating + movie_data.count_rating.score) where movie_data.count_rating.mid = new.mid;
    update movie_data.count_rating set movie_data.count_rating.count=(1 + movie_data.count_rating.count) where movie_data.count_rating.mid = new.mid;
end//
delimiter ;

delimiter //
create trigger rating_update
after update
on ratings
for each row
begin
    update movie_data.count_rating set movie_data.count_rating.score=(new.rating - old.rating + movie_data.count_rating.score) where movie_data.count_rating.mid = new.mid;
end//
delimiter ;

delimiter //
create trigger movies_insert
after insert
on movies
for each row
begin
    insert into movie_data.count_rating(mid) VALUES (new.mid);
end//
delimiter ;

delimiter //
create trigger count_rating_update
after update
on count_rating
for each row
begin
    if new.count != 0
    then
        update movies set movies.score=(new.score / new.count) where movies.mid=new.mid;
    end if;
end//

delimiter ;


# select * from users;
# select * from movies;
# select * from ratings;
# select * from count_rating;
#select mid, count(mid) as count ,rating_time from ratings group by mid order by rating_time DESC;

/*
use  movie_data;
INSERT INTO users(user_name, password, occupation, sex, age) VALUES ('aaaa', '123', '学生', '男', '21');
insert into users(uid, user_name, password)
VALUES(1, 'aaa1', '12');
insert into movies(mid, name, genres)
VALUES(1193, 'aaaabbbbb', 'aaaaaab11111');
insert into ratings(uid, mid, rating, rating_time)
VALUES(1, 1193, 5, '2001-01-01 06:12:40');
insert into users(uid, user_name, password)
VALUES(2, 'aaa112', '12');
insert into movies(mid, name, genres)
VALUES(661, 'aaaabcccc', 'aaaaddb11111');
insert into ratings(uid, mid, rating, rating_time)
VALUES(1, 661, 3, '2001-01-01 06:35:09');
insert into ratings(uid, mid, rating, rating_time)
VALUES(2, 661, 1, '2001-01-01 06:35:09');
*/
# drop database movie_data;