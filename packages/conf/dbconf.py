# encoding=utf-8

"""
    @describe: 这是一个数据库初始化配置文件
"""

DATABASE_NAMES = ["movie_data"]
TABLES = [
    {
        "name": "movies",
        "col": [
            "mid",
            "name"
            "timelong",
            "issue",
            "shoot",
            "language",
            "descri"
            "actors",
            "directors",
            "score",
            "genres"
        ]
    },
    {
        "name": "ratings",
        "col": [
            "uid",
            "mid",
            "rating",
            "rating_time"
        ]
    },
    {
        "name": "users",
        "col": [
            "uid",
            "user_name",
            "password",
            "first",
            "create_time",
            "occupation",
            "sex",
            "age"
        ]
    },
    # {
    #     "name": "top_movies",
    #     "col": [
    #         "update_time",
    #         "genres",
    #         "smid",
    #         "score"
    #     ]
    # },
    {
        "name": "tag",
        "col": [
            "mid",
            "uid",
            "tag",
            "time"
        ]
    },
    {
        "name": "user_recs",
        "col": [
            "uid",
            "smid",
            "score"
        ]
    },
    {
        "name": "stream_recs",
        "col": [
            "uid",
            "smid",
            "score"
        ]
    },
    {
        "name": "u_like",
        "col": [
            "mid",
            "uid",
            "rank_score"
        ]
    },
    {
        "name": "movie_recs",
        "col": [
            "mid",
            "smid",
            "score"
        ]
    }
]

DATA_DICT = {
    "movieId": "mid",
    "Title": "name",
    "Genres": "genres",
    "userId": "uid",
    "UserID": "uid",
    "timestamp": "rating_time",
    "rating": "rating",
    "Occupation": "occupation",
    "Age": "age",
    "Gender": "sex",
    "Zip-code": "user_name"
}
DATA_DICT1 = {
    "movieId": "mid",
    "Title": "name",
    "Genres": "genres",
    "userId": "uid",
    "UserID": "uid",
    "timestamp": "time",
    "tag": "tag",
    "rating": "rating",
    "Occupation": "occupation",
    "Age": "age",
    "Gender": "sex",
    "Zip-code": "user_name"
}



DB_DRIVER = 'com.mysql.jdbc.Driver'
DB_USER = 'hive'
DB_PASSWORD = '123456'
DB_HOST = "hadoop103"
DB_DATABASE = "movie_data"
DB_URL = 'jdbc:mysql://hadoop103:3306/movie_data?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai&useSSL=false&user={}&password={}&useUnicode=true&characterEncoding=UTF-8'.format(DB_USER, DB_PASSWORD)

# users 编码
# 职业
USER_DECODE_OCCUPATION = {
    "0": "'其他'或未指定",
    "1": "学术/教育家",
    "2": "艺术家",
    "3": "文员/管理",
    "4": "大学生/研究生",
    "5": "客户服务",
    "6": "医生/医疗保健",
    "10": "K-12 学生",
    "11": "律师",
    "12": "程序员",
    "13": "退休",
    "14": "销售/营销",
    "15": "科学家",
    "16": "个体经营者",
    "17": "技术员/工程师",
    "18": "工匠/工匠",
    "19": "失业",
    "20": "作家",
    "7": "未知",
    "8": "未知",
    "9": "未知"

}
USER_DECODE_AGE = {
    "1": "Under 18",
    "18": "18-24",
    "25": "25-34",
    "35": "35-44",
    "45": "45-49",
    "50": "50-55",
    "56": "56+"
}
USER_DECODE_SEX = {
    "M": "男",
    "F": "女"
}

# 电影类别
GENRES = ["Action",
          "Adventure",
          "Animation",
          "Comedy",
          "Crime",
          "Documentary",
          "Drama",
          "Family",
          "Fantasy",
          "Foreign",
          "History",
          "Horror",
          "Music",
          "Mystery",
          "Romance",
          "Science",
          "Tv",
          "Thriller",
          "War",
          "Western"
          ]