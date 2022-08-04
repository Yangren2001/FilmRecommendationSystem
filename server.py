# encoding = utf-8

import flask
from flask import request, redirect, url_for, flash, make_response
from urllib.parse import quote, unquote
import os
import json
import time
import socket
import threading
from packages.DB.DB import DB
from packages.conf.realtimeconf import *


class DB_A(DB):
    def __init__(self):
        pass

    def test(self):
        pass
# print(os.getcwd() + "/")
app = flask.Flask(os.getcwd() + "/")

def load(pathfile):
    with open(pathfile, "rb") as fp:
        return fp.read()


def GetMovieName(ls, col=None, where=""):
    db = DB_A()
    db.connect()
    sql = None
    if col is None:
        sql = "SELECT * FROM movies WHERE mid={}" + where
    else:
        col = ",".join(col)
        sql = "SELECT " + col + " FROM movies WHERE mid={}" + where
    return_ = []
    for i in ls:
        data = db.select(sql.format(i[0]))[0]
        return_.append(data)
    return return_


@app.route('/', methods=['GET', 'POST'])
def index():
    return app.send_static_file("html/index.html/")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == "POST":
        user = request.form.get("user")
        pwd = request.form.get("pwd")
        db = DB_A()
        db.connect()
        sql = "SELECT uid, user_name, password FROM users WHERE user_name='{}'".format(user)
        data = db.select(sql)
        db.close()
        # print(data)
        if data is None:
            return json.dumps({"login": False})
        else:
            if user == data[0][1] and pwd == data[0][2]:
                resp = make_response(json.dumps({"login": True}), 200)
                resp.set_cookie("uid", str(data[0][0]), 60 * 60 * 12)
                resp.set_cookie("login", "1", 60 * 60 * 12)
                return resp

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == "POST":
        data = request.form.to_dict()
        db = DB_A()
        db.connect()
        sql = "SELECT * FROM users WHERE user_name='{}'".format(data["user_name"])
        data1 = db.select(sql)
        if data1 is not None:
            return json.dumps({"register_state": False, "register_info": quote("用户名已存在！")})
        else:
            k = list(data.keys())
            v = list(data.values())
            sql1 = "INSERT INTO users({}, {}, {}, {}, {}, first ) VALUES ('{}', '{}', '{}', '{}', '{}', TRUE)".format(*k, *v)
            flag = db.sql(sql1)
            db.close()
            if flag:
                return json.dumps({"register_state": True, "register_info": quote("注册成功！")})
            else:
                return json.dumps({"register_state": False, "register_info": quote("注册失败，请联系管理员！")})

@app.route('/movie_genres/<genres>', methods=['GET', 'POST'])
def MovieGenres(genres):
    sql = "SELECT recs FROM GenresTopMovies WHERE genres='{}'".format(genres)
    db = DB_A()
    db.connect()
    data = db.select(sql)
    db.close()
    if data is None:
        return json.dumps({"state": False, "data": None})
    data = eval(data[0][0])
    data = GetMovieName(data, col=["mid", "name"])
    return json.dumps({"state": True, "data": data})


@app.route('/genres', methods=['GET', 'POST'])
def genres():
    if request.method == "POST":
        # g = request.form.get("genres")   # 获取表单数据
        cookie_uid = request.cookies.get("uid")
        db = DB_A()
        db.connect()
        # 查询离线数据
        sql1 = "SELECT recs FROM userRecs WHERE uid={}".format(cookie_uid)
        data = db.select(sql1)
        sql2 = "SELECT recs FROM streamRecs WHERE uid={}".format(cookie_uid)
        real_data = db.select(sql2)  # 实时数据
        db.close()
        if data is None and real_data is None:
            return json.dumps({"data": None,
                               "data_state": False,
                               "real_data": None,
                               "real_data_state": False
                               })
        elif real_data is None:
            data = eval(data[0][0])
            data = GetMovieName(data, col=["mid", "name"])
            return json.dumps({"data": data,
                               "data_state": True,
                               "real_data": None,
                               "real_data_state": False
                               })
        elif data is None:
            real_data = eval(real_data[0][0])
            real_data = GetMovieName(real_data, col=["mid", "name"])
            return json.dumps({"data": None,
                               "data_state": False,
                               "real_data": real_data,
                               "real_data_state": True
                               })
        else:
            data = eval(data[0][0])
            data = GetMovieName(data, col=["mid", "name"])
            real_data = eval(real_data[0][0])
            real_data = GetMovieName(real_data, col=["mid", "name"])
            return json.dumps({"data": data,
                               "data_state": True,
                               "real_data": real_data,
                               "real_data_state": True
                               })

    return json.dumps(GENRES_DICT)

@app.route("/search/", methods=['GET', 'POST'])
def search():
    name = unquote(request.url).split("?")[1].split("=")[1]
    sql = "SELECT * FROM movies WHERE name='{}'".format(name)
    db = DB_A()
    db.connect()
    data = db.select(sql)
    if data is None:
        return json.dumps({"state": False, "data": None})
    return json.dumps({"state": True, "data": data[0][0]})


@app.route("/page/<mid>", methods=['GET', 'POST'])
def showMovies(mid):
    if request.method == "POST":
        db = DB_A()
        db.connect()
        sql = "SELECT  * FROM movies WHERE mid={}".format(mid)
        data = db.select(sql)
        db.close()
        if data is not None:
            return json.dumps(
                {
                    "state": True,
                    "data": data[0]
                }
            )
        else:
            return json.dumps(
                {
                    "state": False,
                    "data": None
                }
            )
    return app.send_static_file("html/show.html/")


@app.route("/rating/<mid>", methods=['GET', 'POST'])
def rating(mid):
    uid = request.cookies.get("uid")
    global counts
    db = DB_A()
    db.connect()
    if request.method == "POST":
        r = request.form.get("rating")
        p = os.getcwd()
        cmd = "sh " + p + "/bin/send.sh "
        sql1 = "INSERT INTO ratings(uid, mid, rating) VALUES ({}, {}, {})".format(uid, mid, r)
        flag = db.sql(sql1)
        db.close()
        if flag:
            with open(p + "/src/log/test.log", "w") as f:
                f.write(("$" + uid + "|" + mid + "|" + r + "|" + str(time.time()) + "$"))
            os.system(cmd)
            return json.dumps({"state": True})
        else:
            return json.dumps({"state": False})


    sql = "SELECT rating FROM ratings WHERE uid={} AND mid={}".format(uid, mid)
    is_u = db.select(sql)
    db.close()
    if is_u is None:
        return json.dumps({"state": False, "rating": None})
    else:
        return json.dumps({"state": True, "rating": is_u[0][0]})



@app.route('/<file>/')
def html(file):
    if file == "index.html" or file == "register.html":
        return app.send_static_file("html/" + file + "/")
    else:
        return app.send_static_file("html/" + file + "/")

@app.route("/image/<path>/")
def image(path):
    filepath = "image/" + unquote(path)
    return app.send_static_file(filepath + "/")

@app.route("/image/<path>/<file>/")
def image1(path, file=None):
    filepath = "image/"
    if file:
        filepath += unquote(path) + "/" + file
    else:
        filepath += unquote(path)
    return app.send_static_file(filepath + "/")

@app.route("/css/<file>/")
def css(file):
    return app.send_static_file("css/" + file + "/")

@app.route("/js/<file>/")
def js(file):
    return app.send_static_file("js/" + file + "/")


def exec():
    os.system("sh " + os.getcwd() + "/bin/start.sh")
    count = 0
    while count < 10000:
        time.sleep(60)
        count += 1


if __name__ == "__main__":
    t1 = threading.Thread(target=exec)
    t1.setDaemon(True)
    t1.start()
    app.run("0.0.0.0", 80, debug=True)