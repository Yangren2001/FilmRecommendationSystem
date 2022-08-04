# encoding = utf-8

HOST_PORT = {
                "master":{
                    "host":"hadoop103",
                    "port":9643
                },
                "work1":{
                    "host":"hadoop104",
                    "port":9092
                },
                "work2":{
                    "host": "hadoop105",
                    "port": 9643
                }
}

INTERVAL = 2
BROKERS = "hadoop103:9092,hadoop104:9092,hadoop105:9092"

GENRES_DICT = {
    "动作": "Action",
    "冒险": "Adventure",
    "动画": "Animation",
    "喜剧": "Comedy",
    "犯罪": "Crime",
    "纪录片": "Documentary",
    "戏剧": "Drama",
    "家庭": "Family",
    "幻想": "Fantasy",
    "外国": "Foreign",
    "历史": "History",
    "恐怖": "Horror",
    "音乐": "Music",
    "神秘": "Mystery",
    "浪漫": "Romance",
    "科学": "Science",
    "电视": "Tv",
    "惊悚片": "Thriller",
    "战争": "War",
    "西部": "Western"
}
